import unittest
import pandas as pd
import sys
import os


current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)

from dags.common.commons import (convert_int_to_percent, copy_columns, drop_columns, read_file, convert_to_date, 
                          replace_nan_with_column_value, 
                        update_and_rename_columns,
                        generate_membrane_column_from_image_name,
                        date_data)


class TestReadFile(unittest.TestCase):

    def test_read_file(self):
        test_data_path = "E:\Yash-Spore.bio-test\data\input.xlsx"
        membrane_df, images_df = read_file(test_data_path)

        self.assertIsNotNone(membrane_df)
        self.assertIsNotNone(images_df)

        self.assertIsInstance(membrane_df, pd.DataFrame)
        self.assertIsInstance(images_df, pd.DataFrame)


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
        membrane_data = pd.DataFrame({
            'membrane_name': ["MEM1", "MEM2", "MEM3"]
        })
        images_data = pd.DataFrame({
            'image_name': ["IMG_MEM1", "IMG_MEM2", "IMG_MEM3_IMG"]
        })
        result = generate_membrane_column_from_image_name(membrane_data, images_data)
        expected_result = pd.DataFrame({
            'image_name': ["IMG_MEM1", "IMG_MEM2", "IMG_MEM3_IMG"],
            'membrane': ["MEM1", "MEM2", "MEM3"]
        })
        pd.testing.assert_frame_equal(result, expected_result)

    def test_date_data(self):
        membrane_images = pd.DataFrame({
            'filtration_date': ['210101', '210102', '210103']
        })
        expected_output = pd.DataFrame({
            'filtration_date': pd.to_datetime(['210101', '210102', '210103'], format='%y%m%d'),
            'date_day': [1, 2, 3],
            'date_month': [1, 1, 1],
            'date_year': [2021, 2021, 2021]
        })
        output = date_data(membrane_images)
        pd.testing.assert_frame_equal(output, expected_output)
 

    def test_copy_columns(self):
        source_df = pd.DataFrame({
            'A': [1, 2, 3],
            'B': ['a', 'b', 'c']
        })
        target_df = pd.DataFrame()
        columns_to_copy = ['A', 'B']
        expected_output = pd.DataFrame({
            'A': [1, 2, 3],
            'B': ['a', 'b', 'c']
        })
        output = copy_columns(source_df, columns_to_copy, target_df)
        pd.testing.assert_frame_equal(output, expected_output)


    def test_convert_int_to_percent(self):
        dataframe = pd.DataFrame({
            'A': [1, 2, 3],
            'B': [4, 5, 6]
        })
        columns_to_clean = ['A', 'B']
        expected_output = pd.DataFrame({
            'A': [100, 200, 300],
            'B': [400, 500, 600]
        })
        output = convert_int_to_percent(dataframe, columns_to_clean)
        pd.testing.assert_frame_equal(output, expected_output)

if __name__ == '__main__':
    unittest.main()