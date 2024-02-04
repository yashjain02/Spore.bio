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


columns_to_drop_membrane = ['acquisitions_realized', 'filtration_date', 'types_of_microorganisms',
                            'matrix_dilution', 'total_number_of_bacteria_measured_in_lab',
                            'nomenclature_format', 'number_of_acquisitions', 'ecoli_percentage',
                            'pseudomonas_percentage', 'pretreatment_operator']


columns_to_drop_images = ['number_of_bacteria_pixels', 'optical_setup', 'acquisitions_realized',
                         'nomenclature_format', 'filtration_date', 'types_of_microorganisms',
                         'matrix_dilution', 'pseudomonas_percentage',
                         'total_number_of_bacteria_measured_in_lab', 'ecoli_percentage',
                         'pretreatment_operator', 'optical_setup', 'lens_diameter',
                         'number_of_acquisitions', 'objective', 'camera']


images_data_to_membrane_images_camera = ['image_name', 'number_of_bacteria_pixels', 'optical_setup', 
                            'acquisitions_realized', 'filtration_date', 'types_of_microorganisms', 
                            'matrix_dilution', 'total_number_of_bacteria_measured_in_lab', 
                            'pseudomonas_percentage','nomenclature_format', 'ecoli_percentage', 
                            'number_of_acquisitions', 'pretreatment_operator']


images_data_to_camera = ['optical_setup','lens_diameter','objective','camera']

columns_to_remove_symbol = ['ecoli_percentage','pseudomonas_percentage']

connection = psycopg2.connect(
    database='spore',
    user='airflow',
    password='airflow',
    host='postgres'
    )
engine = create_engine(f'postgresql://airflow:airflow@postgres:5432/spore')


def read_file(data_path : str) -> pd.DataFrame:
    """
    This Function reads the excel file and returns the data in dataframe
    Args:
        data_path: Path of datalake where raw file is stored.
    """
    excel_file = pd.ExcelFile(data_path)
    membrane_df = pd.read_excel(excel_file, sheet_name='Membranes')
    images_df = pd.read_excel(excel_file, sheet_name='Images')
    return membrane_df, images_df


def convert_to_date(data : pd.DataFrame, column : str):
    """
    This Function takes dataframes and convert the column in date format
    Args:
        dfs: The dataframes passed
        column: name of the column whose datatype needs to be changed
    """
    return data.assign(filtration_date=pd.to_datetime(data[column], format='%y%m%d'))


def replace_nan_with_column_value(dataframe : pd.DataFrame, column_to_fill : str, column_to_use : str) -> pd.DataFrame:
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


def update_and_rename_columns(dataframe : pd.DataFrame, common_column_name, specific_column_name) -> pd.DataFrame:
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
        dataframe.rename(columns=common_column_name, inplace=True)
    if specific_column_name is not None:
        specific_column_name.update(common_column_name)
        dataframe.rename(columns=specific_column_name, inplace=True)
    return dataframe


def generate_membrane_column_from_image_name(membrane_data : pd.DataFrame, membrane_images_camera : pd.DataFrame) -> pd.DataFrame:
    """
    This Function geneartes membrane column with help of image_name. To form a relation with membrane Table.
    Because image_name is extension of
    Args:
        membrane_data: Data of membrane sheet.
        images_data: Data of images sheet.
    """
    for idx, row in membrane_images_camera.iterrows():
        substring = ''
        substing_length = 0
        for membrane_name in membrane_data['membrane_name']:
            if membrane_name in row['image_name']:
                substring_length = len(membrane_name)
                if substring_length > substing_length:
                    substring = membrane_name
                    substing_length = substring_length
        membrane_images_camera.at[idx, 'membrane'] = substring
    return membrane_images_camera


def drop_columns(data : pd.DataFrame, columns_to_drop : list):
    """
    Drop columns from a DataFrame.
    
    Args:
    - df (DataFrame): The DataFrame from which to drop columns.
    - columns_to_drop (list of str): List of column names to drop.
    
    Returns:
    - DataFrame: DataFrame with specified columns dropped.
    """
    return data.drop(columns=columns_to_drop, inplace=True)


def copy_columns(source_dataframe : pd.DataFrame, columns_to_copy : list, target_dataframe : pd.DataFrame) ->pd.DataFrame:
    """
    Copy specific columns from source DataFrame to target DataFrame.
    
    Args:
    - source_df (DataFrame): The DataFrame from which to copy columns.
    - columns_to_copy (list of str): List of column names to copy.
    - target_df (DataFrame): The DataFrame to which the columns will be copied.
    
    Returns:
    - None
    """
    target_dataframe = source_dataframe[columns_to_copy].copy()
    return target_dataframe


def date_data(membrane_images_camera : pd.DataFrame) -> pd.DataFrame:
    """
    This Function creates date dateframe and sperates day,  month and year from date.
    Args:
        membrane_images_camera: membrane_images_camera fact table
    
    Return:
        returns date table
    """
    date_table=pd.DataFrame()
    date_table['filtration_date'] = pd.to_datetime(membrane_images_camera['filtration_date'], format='%y%m%d')
    date_table['date_day'] = date_table['filtration_date'].dt.day
    date_table['date_month'] = date_table['filtration_date'].dt.month
    date_table['date_year'] = date_table['filtration_date'].dt.year
    date_table = date_table.drop_duplicates()
    date_table_data = date_table.dropna()
    return date_table_data


def convert_int_to_percent(dataframe : pd.DataFrame, columns_to_clean: list) -> pd.DataFrame:
    """
    converts decimal or int to percent value from specified columns in the DataFrame.
    
    Args:
    - df (DataFrame): The DataFrame from which to remove the percentage symbol.
    - columns_to_clean (list of str): List of column names to clean.
    
    Returns:
    - None
    """
    for col in columns_to_clean:
            dataframe[col] = dataframe[col].fillna(0)
            dataframe[col]=dataframe[col]*100
    return dataframe


def run_sql_file(sql_file: str)->None:
    """
    This Function runs the sql files.
    Args:
        sql_file: Name of SQL file that need to be run/
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    queries_file_path = os.path.join(current_dir, sql_file)
    with open(queries_file_path, 'r') as file:
        sql_queries = file.read()
    with connection.cursor() as cursor:
        cursor.execute(sql_queries)
    connection.commit()


def insert_to_database(membrane_dimension : pd.DataFrame, images_dimension : pd.DataFrame,
                        camera_dimension : pd.DataFrame, membrane_images_camera : pd.DataFrame, date_dimension:pd.DataFrame) -> None:
    """
    Insert the data to the database
    Args:
        membrane_df: membrane data to be inserted.
        images_df: image data to be inserted
    """

    run_sql_file('create_queries.sql')
    membrane_dimension.to_sql('membrane_dimension', engine, schema='spore', if_exists='append', index=False)
    images_dimension.to_sql('images_dimension', engine, schema='spore', if_exists='append', index=False)
    camera_dimension.to_sql('camera_dimension', engine, schema='spore', if_exists='append', index=False)
    date_dimension.to_sql('date_dimension', engine, schema='spore', if_exists='append', index=False)
    membrane_images_camera.to_sql('membrane_image_camera', engine, schema='spore', if_exists='append', index=False)
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
        cursor.execute(f"SELECT {column_name} FROM spore.{table_name}")
        rows = cursor.fetchall()
        for row in rows:
            data.append(row[0]) 
    return data


def generate_and_save_barcode(data, output_folder : str) -> None:
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
    membrane_data = fetch_data_from_database('membrane_dimension', 'membrane_name', connection)
    images_data = fetch_data_from_database('images_dimension', 'image_name', connection)
    generate_and_save_barcode(membrane_data, 'membrane_barcodes')
    generate_and_save_barcode(images_data, 'images_barcodes')


def schema_setup(membrane_data : pd.DataFrame, images_data : pd.DataFrame) -> pd.DataFrame:
    """
    This Function creates a structure for snowflake schema. It does all the processing and sets the dataframe.

    Args:
        membrane_data: dataframe for membrane.
        images_data: dataframe for images.

    Returns:
        Returns all the dataframe required for tables creation.

    """
    membrane_images_camera=camera_dimension=[]
    # copy data from images to membrane_images_camera for fact table
    membrane_images_camera=copy_columns(images_data, images_data_to_membrane_images_camera, membrane_images_camera)
    # copying images column to camera table
    camera_dimension=copy_columns(images_data, images_data_to_camera, camera_dimension)
    # removing duplicates
    camera_dimension = camera_dimension.drop_duplicates()
    # droping copied columns from images
    drop_columns(images_data, columns_to_drop_images)
    # droping copied column from membrane, since its in fact table
    drop_columns(membrane_data, columns_to_drop_membrane)
    # creating date dimension
    date_dimension = date_data(membrane_images_camera)
    return membrane_data, images_data, membrane_images_camera, camera_dimension, date_dimension


def data_transofmation(data_path: str) -> None:
    """
    This function reads excel file and data transformation required, like change in type, column rename, value rename and
     insert to database.
    Args:
        data_path: Path to the excel file

    """
    membrane_data, images_data = read_file(data_path) #read excel file

    # change the column name with proper names
    membrane_data = update_and_rename_columns(membrane_data,common_column_name, specific_column_name=membrane_column_name)
    images_data = update_and_rename_columns(images_data,common_column_name, specific_column_name=images_column_name)

    # change the value to correct one.
    images_data['usable_for_ml'] = images_data['usable_for_ml'].replace('FAUX', False)

    # Assigns image and membrane to barcode value
    membrane_data = replace_nan_with_column_value(membrane_data, 'barcode', 'membrane_name')
    images_data = replace_nan_with_column_value(images_data, 'barcode', 'image_name')

    # Designs the star schema
    membrane_data, images_data, membrane_images_camera, camera_dimension, date_dimension = schema_setup(membrane_data, images_data)

    # changes the value of ecoli and pseudomonas to percent value than decimal
    membrane_images_camera = convert_int_to_percent(membrane_images_camera, columns_to_remove_symbol)
    membrane_images_camera = convert_to_date(membrane_images_camera, column='filtration_date')

    # generates membrane name from image name for assigning relation membrane table
    membrane_images_camera = generate_membrane_column_from_image_name(membrane_data, membrane_images_camera)
    # inserts to database
    insert_to_database(membrane_data, images_data, camera_dimension, membrane_images_camera, date_dimension)