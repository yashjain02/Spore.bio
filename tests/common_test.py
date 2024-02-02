import unittest
from unittest import mock
import pandas as pd
import sys
import os
from unittest.mock import MagicMock, patch

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)

from dags.commons import (read_file, convert_to_date, 
                          replace_nan_with_column_value, 
                        update_and_rename_columns,
                        fetch_data_from_database)
class TestReadFile(unittest.TestCase):


    def test_read_file(self):
        test_data_path = "E:\Yash-Spore.bio-test\data\input.xlsx"
        membrane_df, images_df = read_file(test_data_path)

        self.assertIsNotNone(membrane_df)
        self.assertIsNotNone(images_df)

        self.assertIsInstance(membrane_df, pd.DataFrame)
        self.assertIsInstance(images_df, pd.DataFrame)
    

    def test_convert_to_date(self):
        df1 = pd.DataFrame({'date_column': ['210101', '220202', '230303']})
        df2 = pd.DataFrame({'date_column': ['200101', '201010', '210303']})
        converted_dfs = convert_to_date(df1, df2, column='date_column')
        self.assertIsInstance(converted_dfs, list)
        self.assertEqual(len(converted_dfs), 2)
        for df in converted_dfs:
            self.assertIsInstance(df, pd.DataFrame)
        for df in converted_dfs:
            self.assertIn('filtration_date', df.columns)
        for df in converted_dfs:
            self.assertEqual(df['filtration_date'].dtype, 'datetime64[ns]')


    def test_replace_nan_with_column_value(self):
        data = {'A': [1, 2, None, 4],
                'B': [10, None, 30, None]}
        df = pd.DataFrame(data)
        filled_df = replace_nan_with_column_value(df, column_to_fill='A', column_to_use='B')
        self.assertIsInstance(filled_df, pd.DataFrame)
        self.assertFalse(filled_df['A'].isnull().any())
    

    def test_update_and_rename_columns(self):
        data = {'col1': [1, 2, 3],
                'col2': [4, 5, 6]}
        df = pd.DataFrame(data)

        common_columns = {'col1': 'new_col1'}
        specific_columns = {'col2': 'new_col2', 'col3': 'new_col3'}
        updated_df = update_and_rename_columns(df, common_columns, specific_columns)

        self.assertIsInstance(updated_df, pd.DataFrame)
        self.assertIn('new_col1', updated_df.columns)
        self.assertIn('new_col2', updated_df.columns)
        self.assertNotIn('col1', updated_df.columns)
        self.assertNotIn('col2', updated_df.columns)

    
    def test_fetch_data_from_database(self):
        mock_connection = MagicMock()

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1,), (2,), (3,)]
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        table_name = 'test_table'
        column_name = 'test_column'
        data = fetch_data_from_database(table_name, column_name, mock_connection)

        mock_cursor.execute.assert_called_once_with(f"SELECT {column_name} FROM {table_name}")
        self.assertEqual(data, [1, 2, 3])
    

if __name__ == '__main__':
    unittest.main()