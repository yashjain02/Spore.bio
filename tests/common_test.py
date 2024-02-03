import unittest
import pandas as pd
import sys
import os


current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)

from dags.common.commons import (read_file, convert_to_date, 
                          replace_nan_with_column_value, 
                        update_and_rename_columns,
                        generate_membrane_column_from_image_name)
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
    
    def test_generate_membrane_column_from_image_name(self):
        # Mock membrane data
        membrane_data = pd.DataFrame({
            'membrane_name': ["MEM1", "MEM2", "MEM3"]
        })
        # Mock images data
        images_data = pd.DataFrame({
            'image_name': ["IMG_MEM1", "IMG_MEM2", "IMG_MEM3_IMG"]
        })
        # Call the function
        result = generate_membrane_column_from_image_name(membrane_data, images_data)
        # Expected result
        expected_result = pd.DataFrame({
            'image_name': ["IMG_MEM1", "IMG_MEM2", "IMG_MEM3_IMG"],
            'membrane': ["MEM1", "MEM2", "MEM3"]  # Expect last row to be empty because no membrane name is found
        })
        # Assert that the result matches the expected result
        pd.testing.assert_frame_equal(result, expected_result)
 

if __name__ == '__main__':
    unittest.main()