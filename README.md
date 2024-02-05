# Spore.Bio

This README provides an overview and usage guide for a set of airflow that facilitate data retrieval, transformation, and storage. These functions are intended to assist in fetching data from an given excel file, transforming it into a structured format, and storing it as Postgres database. 

## About 
This Project intents to ease the work of micrbiologist by providing them structured data and making easily available to them.<br />
We have Excel file in data folder<br />
We will do required transformation and send data to postgreSQL<br />


## Files 
    Yash-spore.bio-test/
    |-dags/
    |     |_ commons/
    |     |         |_commons.py
    |     |         |_create_queries.sql
    |     |         |_graph.py
    |     | 
    |     |_ main.py
    |  
    |-data/
    |     |_ input.xlsx
    |-results/
    |       |_image_barcodes/     
    |       |_membrane_barcode/
    |                 
    |-pgadmin/                
    |     
    |-Tests/
    |      |_test.py
    |      |_common_test.py
    |-logs/
    |-plugins/
    |-Readme.md
    |-requirement.txt
    |-docker-compose.yaml
    |-.env
    

## About the Files
- **main.py**:<br />
    This file is the main file where the script for airflow which contains dag and its operators. The code access functions which are in commons.py file.

- **commons.py**:<br />
    This file contains only functions which are used by main.py. Some are generic function and some are very specfic to this project. The function definetion are below.<br />

- **graph.py**:<br />
    This file produces the graph for "the number of bacteria per membrane VS the average surface occupied by bacteria"<br />

- **create_queries.sql**:<br />
    This file contains SQL DCL queries which are used to create tables and schema in this project.<br />

- **common_test.py**:<br />
    This file contains unit test for all common file functions.<br />

- **test.py:**<br />
    This file contains test case for airflow dag.<br />

## Functions used in this project<br />
- **read_file(data_path: str) -> pd.DataFrame:**<br />
    This Function reads the excel file and returns the data in dataframe<br />

    Parameters:<br />
    data_path: Path of datalake where raw file is stored.


    Returns:<br />
    Writes raw data in dataframe format.<br />

- **convert_to_date(*dataframes: pd.DataFrame, column: str)->None:**<br />
    This Function takes dataframes and convert the column in date format.<br />

    Parameters:<br />
    dataframes: The dataframes passed<br />
    column: name of the column whose datatype needs to be changed<br />

    Output:<br />
    Returns dataframe with conveted datetime column<br />

- **replace_nan_with_column_value(dataframe, column_to_fill: str, column_to_use: str) -> pd.DataFrame:**<br />
    Replace NaN values in a column with corresponding values from another column in the same DataFrame.<br />

    Parameter:<br />
    dataFrame: The DataFrame containing the columns.<br />
    column_to_fill (str): The name of the column with NaN values to replace.<br />
    column_to_use (str): The name of the column to use for replacement.<br />   
    Output:<br />
    DataFrame: The DataFrame with NaN values replaced.<br />

- **update_and_rename_columns(df, common_column_name, specific_column_name) -> pd.DataFrame:**<br />
    Update and rename columns in a DataFrame.<br />

    Args:<br />
        dataframe (DataFrame): The DataFrame to update and rename columns for.<br />
        common_column_name (dict): A dictionary containing common column names to update.<br />
        specific_column_name (dict): A dictionary containing specific column names to update.<br />

    Returns:<br />
        DataFrame: The DataFrame with updated and renamed columns.<br />
    
- **data_transofmation(data_path: str) -> None:**<br />
    This function reads excel file and data transformation required, like change in type, column rename, value rename and
    insert to database.<br />

    Args:<br />
    data_path: Path to the excel file.<br />

    Returns:<br />
        Save the data in database.

- **insert_to_database(membrane_data:pd.DataFrame, images_data:pd.DataFrame,camera:pd.DataFrame, membrane_images:pd.DataFrame, date:pd.DataFrame) -> None::**<br />

    Insert the data to the database.<br />

    Args:<br />
        membrane_df: membrane data to be inserted.<br />
        images_df: image data to be inserted.<br />

    Returns:<br />
        data is been inserted to the database

- **fetch_data_from_database(table_name: str, column_name: str, connection) -> pd.DataFrame:**<br />
    Fetch data from a specified column of a table in the database.<br />

    Args:<br />
        table_name (str): The name of the table to fetch data from.<br />
        column_name (str): The name of the column to fetch data from.<br />
        connection: The connection to the PostgreSQL database.<br />

    Returns:<br />
        list: A list of data entries fetched from the specified column.<br />

- **generate_and_save_barcode(data, output_folder: str) -> None:**<br />
    Generate barcode images for the provided data and save them to the specified folder.<br/>

    Args:<br />
        data (list): A list of data entries to generate barcodes for.<br />
    Returns:<br />
        Save the barcode image in give path.

- **populate_barcode():->None**
    save barcode images for the provided data and save them to the specified folder.<br />

    Returns:<br />
        Generates and saves the barcode.

- **run_sql_file(sql_file: str)->None:**<br />
    This Function runs the sql files.<br />

    Args:<br />
        sql_file: Name of SQL file that need to be run.<br />

- **generate_membrane_column_from_image_name(membrane_data: pd.Dataframe, images_data: pd.Dataframe) -> pd.DataFrame:**
    This Function geneartes membrane column with help of image_name. To form a relation with membrane Table.
    Because image_name is extension of
    Args:
        membrane_data: Data of membrane sheet.
        images_data: Data of images sheet.

- **drop_columns(data : pd.dataframe, columns_to_drop: list)-> pd.Dataframe:**
    Drop columns from a DataFrame.
    
    Args:
      df (DataFrame): The DataFrame from which to drop columns.
      columns_to_drop (list of str): List of column names to drop.
    
    Returns:
      DataFrame: DataFrame with specified columns dropped.


- **copy_columns(source_dataframe : pd.DataFrame, columns_to_copy : list, target_dataframe : pd.DataFrame) ->pd.DataFrame -> None**
    Copy specific columns from source DataFrame to target DataFrame.
    
    Args:
        source_df (DataFrame): The DataFrame from which to copy columns.
        columns_to_copy (list of str): List of column names to copy.
        target_df (DataFrame): The DataFrame to which the columns will be copied.
    
    Returns:
        None

- **date_data(membrane_images : pd.DataFrame) -> pd.DataFrame: ->pd.Dataframe**
    This Function creates date dateframe and sperates day,  month and year from date.
    Args:
        membrane_images: membrane_images fact table
    
    Return:
        returns date table

- **schema_setup(membrane_data : pd.DataFrame, images_data : pd.DataFrame) -> pd.DataFrame:**
    This Function creates a structure for snowflake schema. It does all the processing and sets the dataframe.

    Args:
        membrane_data: dataframe for membrane.
        images_data: dataframe for images.

    Returns:
        Returns all the dataframe required for tables creation.

- **convert_int_to_percent(dataframe : pd.DataFrame, columns_to_clean: list) -> pd.DataFrame:**
    converts decimal or int to percent value from specified columns in the DataFrame.
    
    Args:
    - df (DataFrame): The DataFrame from which to remove the percentage symbol.
    - columns_to_clean (list of str): List of column names to clean.
    
    Returns:
    - None

## Running the Script

To run the Python script that contains the data processing functions, follow these steps:

1. **Clone the Repository:**
   If you haven't already, clone this repository to your local machine using Git.

   ```bash
   git clone https://github.com/yashjain02/yash-spore.bio-test.git
   cd yash-spore.bio-test

2. **Running the Script**
    ```Python
    docker-compose up --build
    ```
    It would take 5 mins to load the webservers, because you are pulling the image from the start.

3. Before you run the pipeline, login in Pgadmin and delete the existing schema(not the database). It will also delete if table already existed. Because I am storing the session info for quick use next time. I have deleted all the info but there might be a scenario where some session might be left in docker.<br />
To Access the server follow below instruction.<br />
**Access to Pgadmin**<br />
 **Pgadmin**<br />
    ```
    open the browser and enter : http://localhost:15432/
    username: spore@datengineer.com
    password: postgres
    ```
    create one server by clicking on 'add server', you find it in center of you screen,<br />
    **Name of server: Spore**,<br />
    In next tab connections,<br /> 
    **Name of host: postgres**,<br />
    **user : airflow**<br />
    **password:airflow**<br />
    Then save it, you find the name under server on you left side of you screen.<br />
    Under Spore, expand database>spore(if you cannot find right click on database and create a new one under name "Spore")>check if spore schema is present, then delete it. If not you can proceed with execution.

    **Note: If you wish to run the pipeline again then you need to delete the table in pgadmin, Because, we are dealing with single file and already the data would be in table, by inserting again the same data would voilate the primary key integrity and would cause error. Primary key column should have unique value. By adding again the same data by running the pipeline without deleting data, will add duplicates.**

4. **web UI**<br />
    ```
    open the browser and enter : http://localhost:8080
    username: airflow
    password: airflow
    ```

    



## Running the test cases

Unit test cases have been written for both airflow dag and functions
To run the tests for airflow dag, run the below command.<br />
**Make sure docker compose is running**. Open new terminal and execute below command.
```bash
docker exec <container_id> pytest /opt/airflow/tests/test.py
```
container Id: Id of airflow webserver. You can obtain this id by running 
```bash
docker ps
```
**Note: Before running unittest of common_test.py. please cooment the code in commons file(Line No: 61 and 67). Because test scripts will be accessing and running commons file and it does not have capability to translate name to address, only docker has it.**<br />
To run unit test cases. Go to tests folder and run the test file directly or run below command.<br />
```python
cd tests
python -m unittest common_test.py
``` 



## Explaination, Dataflow of my approach

1. I have used docker compose file of airflow where I have modified the file by adding pgadmin service and mounting the folders aswell. I am  using pgadmin as database client.
2. I have written to a Docker file to install the required libraries for this project.
3. When you run docker compose up, the docker first installs the libraries and runs all the services.
4. Go to the address given above for airflow UI, Login with the given credentials. Also login to pgadmin and check if the table is not created under spore database. If table exist then delete the tables so you can the pipelines seemlessly. Because, both table have Primary key and adding same data again will cause error. Run the pipeline in airflow.
5. First it reads the excel file, then it does the required transformation.
6. Transformation like change in type, creating new columns, removing Nan values, replacing incorrect name with corect ones.
7. I have used barcode value to store in database rather than image because, it is the optimized way to store in database, which is aquire less memory and will be easily retreivable by non techical user.
8. Once the transformation is done, table and schema is created if not exist from create_queries.sql file. Data is inserted to database. It inserts the data. I have created env variable for the postgres cred, it is bad practice to expose passwords. You can find the env variable in docker compose.
9. Then in next task, it retreive the barcode data and convert it to the images and save to the local system.
10. You may find barcode images under results folder.

## Why Transformation?:
When We look at the data, there are some wierd column names, Usable_for_ML has Faux for false, some columns have Nan values,
filtration_date is in string, Need to replace barcode value(which is Nan) to image_name or membrane_name. These values can't be inserted in database or fed to model for traning which may result in bad prediction.

## Star Schema:
I have used Star schema in this project because it improves query performance, better data integrity, reduced data redundancy, simple maintaince. And this schema is used for analyzing huge amount of data, and also requires less join statements.
If we see both the sheets in excel, most of the column are common, which is redundant. So I have created a snowflake schema, where membrane, images, date and camera is dimension and membrane_image_camera is fact table. I did not choose experiment as another dimension because in both membrane and images the values of usable for Ml differs and it is dependent on experiment name.
Membrane, images, camera, date are qualitative measure and membrane_image_camera is quantitative measure. dimension table defines image table.


## Why PostgreSQL or SQL?
Because  for the simple reason that the input is in Excel file in structured format. There is no reason to go for NoSQL when we have Structured formated file. And also it provide joins, windows functions for analytical query and helps with complex queries. It also provides scalability and ACID properties. And moreover it is easily used for data viz tools like tableau and PowerBI

## Future Developement

1. Instead of using pandas, spark is best for data processing. It is fast and has in memory and pandas are worst in airflow when it comes for large dataset.
2. We have to use concept for date partitioning and data lake when the dataset exceeds for backfilling and faster readability.
3. We need to have proper data warehouse like redshift, snowflake because of the variety of data.
4. Its best to use data viz tools so that it is easy for non-tech user for access the data.
5. We need to create a data catalog tool so that anybody in the company can access the data info.
6. We need have to create specific indexes for faster data retrevials.
7. Its a best practice to have data contracts, so we can have info about the change in schema.
8. We can use DBT for complex data models.



