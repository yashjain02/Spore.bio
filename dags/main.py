from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from common.commons import data_transofmation,populate_barcode


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}
dag = DAG(
    'membrane_Image_database',
    default_args=default_args,
    description='A DAG to process Excel file and insert data into PostgreSQL',
    schedule_interval=None,
)

send_excel_to_database = PythonOperator(
    task_id='send_excel_to_database',
    python_callable=data_transofmation,
    op_kwargs={'data_path':'/opt/data/input.xlsx'},
    dag=dag,
)

generate_barcode = PythonOperator(
    task_id='generate_barcode_and_save',
    python_callable=populate_barcode,
    dag=dag
)

send_excel_to_database>> generate_barcode
