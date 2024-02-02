# BlaBlaCar

This README provides an overview and usage guide for a set of airflow that facilitate data retrieval, transformation, and storage. These functions are intended to assist in fetching data from an API, transforming it into a structured format, and storing it as CSV files. 

## About 
This Project intents to query data from Public API for "Transport for The Netherlands" which provide information about OVAPI, country-wide public transport and store it in a CSV file.<br />
API Description: https://github.com/koch-t/KV78Turbo-OVAPI/wiki.<br />
We will use Per Line Endpoint<br />
Base_url : http://v0.ovapi.nl/<br />
Endpoint: /line/<br />
Authorization: Not needed <br />

## Files 
    airflow/
    |-dags/
    |     |_ commons.py
    |     |_ main.py 
    |     
    |-results/
    |       |_datalake/
    |       |         |_raw_data.csv
    |       |_data warehouse/
    |                 |_2023/
    |                     |_10/
    |                       |_17/
    |                         |_line_data.csv
    |
    |-Tests/
    |      |_test.py
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

    **Functions**:<br />
    - **fetch_data(url: str, datalake_path :str, rawdata_filename:str, context)->None:**<br />
        This function fetches data from a given API using the provided URL and writes down in csv file in result/datalake folder.It is the raw data<br />

        Parameters:<br />
        url (str): The URL of the API from which data will be fetched.<br />
        datalake_path: Path of datalake where raw file is stored.<br />
        rawdata_filename: Name of the raw data file.<br />
        **context: This parameter gets the task details, like execution date, start date etc.<br />

        Returns:<br />
        Writes raw data in csv format or None in case of errors.<br />
    
    - **data_transformation(datalake_path : str, data_warehouse_path : str, final_file : str, rawdata_file : str, context) ->None**<br />
        This function performs data transformation on the raw_data and writes it to a CSV file in structed format in data warehouse. It un-nests inner dictionaries, adds a main key as a separate column, and writes the data to a CSV file.<br />

        Parameters:<br />
        datalake_path: Path of datalake where raw file is stored.<br />
        data_warehouse_path: path to the datawarehouse folder. <br />
        final_file: Name of the final file stored in data warehouse.<br />
        **context: This parameter gets the task details, like execution date, start date etc.<br />
        rawdata_filename: Name of the raw data file.<br />

        Output:<br />
        Writes the transformed data to a CSV file in the data warehouse folder in date partition.<br />

    - **read_from_csv(datalake_path : str, date_partition : str, rawdata_file : str) -> dict**<br />
      This function reads from csv file and return the output.<br />

      Parameter:<br />
      rawdata_filename: Name of the raw data file.<br />
      datalake_path: Path of datalake where raw file is stored.<br />
      date_partition: File path with date patitioned folder<br />      
       Output:<br />
        Reades data to the specified CSV file and returns the data.<br />

    - **write_to_csv(file_path: str, fieldnames: list, csv_data: list) -> None**<br />
        This function writes data to a CSV file.<br />

        Parameters:<br />
        file_path (str): The path to the CSV file.<br />
        fieldnames (list): Names of the header columns.<br />
        csv_data (list): Data to be written to the CSV file.<br />

        Output:<br />
        Writes data to the specified CSV file.<br />
    
    - **date_partitioning(context) -> str**<br />
        This function creates a directory structure based on the execution date of the dag. It's intended for date-based partitioning of data storage.<br />

        Parameters:<br />
        **context: This parameter gets the task details, like execution date, start date etc.<br />

        Returns:<br />
        str: The path to the directory structure based on the execution date.
        Usage

## Running the Script

To run the Python script that contains the data processing functions, follow these steps:

1. **Clone the Repository:**
   If you haven't already, clone this repository to your local machine using Git.

   ```bash
   git clone https://github.com/yashjain02/BlaBlaCar.git
   cd BlaBlaCar
   cd airflow

2. **Running the Script**
    ```Python
    docker-compose airflow init
    docker-compose up
    ```
3. **web UI**<br />
    ```
    open the browser and enter : http://localhost:8080
    username: airflow
    password: airflow
    
## Running the test cases

Unit test cases have been only written for airflow dags. Commons function testcases are in python_without_airflow.The function perform the same. The test scripts are in test folder in test.py file.
To run the tests run the below command.
```Python
python -m unittest tests/test.py
```

## Data Flow in the script

Below is the step by step process how the data is travelled.
1. By running docker-compose up command in terminal, wait for a min or two to start the web UI. Then enter the address in your browser, given above. enter the username and password.
2. you will find a dag, trigger the dag.
3. In first task, it fetchs the data from API and stores it in result/datalake/{date_partition} folder as a raw data.It creates a date partition folder, because for faster readility and backfilling in airflow
4. I have tried to replicate an actual data pipeline. Since a transformation is required so I have used concept of raw data and datalake.
5. In second task, It reads from raw_file usind read_from_csv() function and send the data to data_transformation()
6.  **Why data tranformation?**<br />
    When you run the airflow script you will find in result/datalake/{}date_partition/raw_data.csv that the data is unstructed. The Line are as headers and the value are dictionary. For data analyst it is difficult to create a dashboard. 
7. In data_transformation(), it creates a date partition folder, because for faster readility and backfilling in airflow. And also faster for data analyst to query the data.
8. Then the data transformation takes place and write the final output in result/datawarehouse/(current_date).


