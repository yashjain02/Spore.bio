import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from barcode import Code128, writer

common_column_name = {
    'experiment_name_(aaaa#what_it?)':'experiment_name',
    'Usable for ML':'usable_for_ml',
    'Exclusion reason':'exclusion_reason',
    'acquisitions_realized?':'acquisitions_realized',
    'filtration_date_yymmdd':'filtration_date',
    'types_of_microorganisms_(AAAA-Bxx)':'types_of_microorganisms',
    'ecoli %':'ecoli_percentage',
    'pseudomonas %':'pseudomonas_percentage',
    'nomenclature_format_(from0tohigher)':'nomenclature_format',  
    'matrix_dilution_(dxx)':'matrix_dilution',
    'total number of bacteria measured in lab (log10)':'total_number_of_bacteria_measured_in_lab',
    }

membrane_column_name = {
    'membrane name': 'membrane_name',
    'row': 'row_num',
    'biosample_position_(0to999999)':'biosample_position', 
}


images_column_name = {
    'image name': 'image_name',
    'number of bacteria pixels': 'number_of_bacteria_pixels',
    'optical setup': 'optical_setup',
    'lens diameter': 'lens_diameter'
}

database = os.environ.get('database')
user= os.environ.get('user')
password = os.environ.get('password')
host = os.environ.get('host')

connection = psycopg2.connect(
    database=database,
    user=user,
    password=password,
    host=host
    )

def read_file(data_path:str) -> pd.DataFrame:
    """
    This Function reads the excel file and returns the data in dataframe
    Args:
        data_path: Path of datalake where raw file is stored.
    """
    excel_file = pd.ExcelFile(data_path)
    membrane_df = pd.read_excel(excel_file, sheet_name='Membranes')
    images_df = pd.read_excel(excel_file, sheet_name='Images')
    return membrane_df, images_df


def convert_to_date(*dataframes: pd.DataFrame, column: str):
    """
    This Function takes dataframes and convert the column in date format
    Args:
        dfs: The dataframes passed
        column: name of the column whose datatype needs to be changed
    """
    return [dataframe.assign(filtration_date=pd.to_datetime(dataframe[column], format='%y%m%d')) for dataframe in dataframes]


def replace_nan_with_column_value(dataframe, column_to_fill:str, column_to_use:str) -> pd.DataFrame:
    """
    Replace NaN values in a column with corresponding values from another column in the same DataFrame.

    Args:
        df (DataFrame): The DataFrame containing the columns.
        column_to_fill (str): The name of the column with NaN values to replace.
        column_to_use (str): The name of the column to use for replacement.

    Returns:
        DataFrame: The DataFrame with NaN values replaced.
    """
    dataframe[column_to_fill] = dataframe[column_to_fill].fillna(dataframe[column_to_use])
    return dataframe


def update_and_rename_columns(df, common_column_name, specific_column_name) -> pd.DataFrame:
    """
    Update and rename columns in a DataFrame.

    Args:
        df (DataFrame): The DataFrame to update and rename columns for.
        common_column_name (dict): A dictionary containing common column names to update.
        specific_column_name (dict): A dictionary containing specific column names to update.

    Returns:
        DataFrame: The DataFrame with updated and renamed columns.
    """
    if common_column_name is not None:
        df.rename(columns=common_column_name, inplace=True)
    if specific_column_name is not None:
        specific_column_name.update(common_column_name)
        df.rename(columns=specific_column_name, inplace=True)
    return df


def data_transofmation(data_path: str) -> None:
    """
    This function reads excel file and data transformation required, like change in type, column rename, value rename and
     insert to database.
    Args:
        data_path: Path to the excel file

    """
    membrane_data, images_data = read_file(data_path)
    membrane_data = update_and_rename_columns(membrane_data,common_column_name, specific_column_name=membrane_column_name)
    images_data = update_and_rename_columns(images_data,common_column_name, specific_column_name=images_column_name)
    images_data['usable_for_ml'] = images_data['usable_for_ml'].replace('FAUX', False)
    membrane_data = replace_nan_with_column_value(membrane_data, 'barcode', 'membrane_name')
    images_data = replace_nan_with_column_value(images_data, 'barcode', 'image_name')
    membrane_data, images_data = convert_to_date(membrane_data, images_data, column='filtration_date')
    insert_to_database(membrane_data, images_data)


def insert_to_database(membrane_data, images_data) -> None:
    """
    Insert the data to the database
    Args:
        membrane_df: membrane data to be inserted.
        images_df: image data to be inserted
    """
    engine = create_engine(f'postgresql://{user}:{password}@{host}:5432/{database}')
    current_dir = os.path.dirname(os.path.abspath(__file__))
    queries_file_path = os.path.join(current_dir, 'queries.sql')
    with open(queries_file_path, 'r') as file:
        sql_queries = file.read()
    with connection.cursor() as cursor:
        cursor.execute(sql_queries)
    connection.commit()
    membrane_data.to_sql('membrane_table', engine, if_exists='append', index=False)
    images_data.to_sql('images_table', engine, if_exists='append', index=False)
    connection.close()


def fetch_data_from_database(table_name: str, column_name: str, connection) -> pd.DataFrame:
    """
    Fetch data from a specified column of a table in the database.

    Args:
        table_name (str): The name of the table to fetch data from.
        column_name (str): The name of the column to fetch data from.
        conn: The connection to the PostgreSQL database.

    Returns:
        list: A list of data entries fetched from the specified column.
    """
    data = []
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT {column_name} FROM {table_name}")
        rows = cursor.fetchall()
        for row in rows:
            data.append(row[0]) 
    return data


def generate_and_save_barcode(data, output_folder: str) -> None:
    """
    Generate barcode images for the provided data and save them to the specified folder.

    Args:
        data (list): A list of data entries to generate barcodes for.
    """
    for code in data:
        code128 = Code128(code, writer=writer.ImageWriter())
        code128.save(f"/opt/results/{output_folder}/barcode_{code}")


def populate_barcode():
    """
    save barcode images for the provided data and save them to the specified folder.

    Args:
        data (list): A list of data entries to generate barcodes for.
        output_folder (str): The folder path where the barcode images will be saved.
    """
    membrane_data = fetch_data_from_database('membrane_table', 'membrane_name', connection)
    images_data = fetch_data_from_database('images_table', 'image_name', connection)
    generate_and_save_barcode(membrane_data, 'membrane_barcodes')
    generate_and_save_barcode(images_data, 'images_barcodes')