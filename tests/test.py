from airflow.models import DagBag
import unittest


def test_dag_structure():
    """
    Test whether the DAG has the expected structure.
    """
    dag_id = 'excel_to_postgres_pipeline'
    dags = DagBag('./dags/membrane.py').get_dag(dag_id)

    # Confirm the DAG and task ID exists
    assert dags is not None
    assert dag_id == dags.dag_id
    assert dags.has_task('send_excel_to_database')
    assert dags.has_task('generate_barcode_and_save')

    # Confirm the tasks are in the correct order
    assert dags.get_task('send_excel_to_database').downstream_task_ids == {'generate_barcode_and_save'}
    assert dags.get_task('generate_barcode_and_save').upstream_task_ids == {'send_excel_to_database'}

if __name__ == '__main__':
    unittest.main()
