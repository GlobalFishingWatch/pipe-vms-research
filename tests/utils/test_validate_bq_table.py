import argparse
import unittest

from pipe_vms_research.utils.bqtools import validate_bq_table


class TestValidateBigQueryTable(unittest.TestCase):
    def test_valid_input_with_project_id(self):
        input_string = 'my_project:my_dataset.my_table'
        expected_output = ('my_project', 'my_dataset', 'my_table')
        self.assertEqual(validate_bq_table(input_string), expected_output)

    def test_valid_input_with_project_id_point_separator(self):
        input_string = 'my_project.my_dataset.my_table'
        expected_output = ('my_project', 'my_dataset', 'my_table')
        self.assertEqual(validate_bq_table(input_string), expected_output)
    
    def test_valid_input_without_project_id(self):
        input_string = 'my_dataset.my_table'
        expected_output = (None, 'my_dataset', 'my_table')
        self.assertEqual(validate_bq_table(input_string), expected_output)
    
    def test_invalid_input_format(self):
        input_string = 'invalid_format'
        with self.assertRaises(argparse.ArgumentTypeError):
            validate_bq_table(input_string)   

    def test_invalid_input_format_no_data(self):
        input_string = ':..'
        with self.assertRaises(argparse.ArgumentTypeError):
            validate_bq_table(input_string)

    def test_invalid_input_format_empty_project(self):
        input_string = ':a.b'
        with self.assertRaises(argparse.ArgumentTypeError):
            validate_bq_table(input_string)
    
    def test_invalid_input_format_too_many_segments(self):
        input_string = ':a.b...'
        with self.assertRaises(argparse.ArgumentTypeError):
            validate_bq_table(input_string)

if __name__ == '__main__':
    unittest.main()
