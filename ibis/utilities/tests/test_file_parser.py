"""Tests for inventory"""
import unittest
import os
from ibis.utilities.file_parser import parse_file_by_sections

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class FileParserTest(unittest.TestCase):
    """Tests the file parser"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_parse_file_by_sections(self):
        """Given a file containing sections with a header.
        Parse sections into a list[{dictionary}]"""
        request_fp = open(os.path.join(BASE_DIR, 'fixtures/request_file.txt'),
                          'r')
        req_keys = ['source_database_name', 'source_table_name']
        optional_keys = ['domain', 'db_username', 'password_file', 'jdbcurl']
        sections, msg, _ = parse_file_by_sections(request_fp, '[Request]',
                                                  req_keys, optional_keys)
        expected = [{'source_database_name': 'fake_database',
                     'jdbcurl': 'jdbc:db2://fake.db2:50400/fake_servicename',
                     'password_file': '/test/passwd', 'db_username': 'fake_username',
                     'source_table_name': 'claim_dim'},
                    {'source_database_name': 'fake_database',
                     'jdbcurl': 'jdbc:db2://fake.db2:50400/fake_servicename',
                     'password_file': '/test/passwd', 'db_username': 'fake_username',
                     'source_table_name': 'claim_test'}]

        self.assertEqual(cmp(sorted(expected), sorted(sections)), 0)

    def test_parse_file_by_sections_spaces(self):
        """test for spaces around user input"""
        _path = os.path.join(BASE_DIR, 'fixtures/request_file_spaces.txt')
        request_fp = open(_path, 'r')
        req_keys = ['source_database_name', 'source_table_name']
        optional_keys = ['domain', 'db_username', 'password_file', 'jdbcurl']
        sections, msg, _ = parse_file_by_sections(request_fp, '[Request]',
                                                  req_keys, optional_keys)
        expected = [{'source_database_name': 'fake_database',
                     'jdbcurl': 'jdbc:db2://fake.db2:50400/fake_servicename',
                     'password_file': '/test/passwd', 'db_username': 'fake_username',
                     'source_table_name': 'claim_dim'}]

        self.assertEqual(cmp(sorted(expected), sorted(sections)), 0)

    def test_parse_file_by_sections_fail(self):
        """Given a file containing sections with a header.
         Header is missing error"""
        request_fp = open(
            os.path.join(BASE_DIR, 'fixtures/request_file_header_missing.txt'),
            'r')
        req_keys = ['source_database_name', 'source_table_name', ]
        optional_keys = ['domain', 'db_username', 'password_file', 'jdbcurl']
        self.assertRaises(ValueError, parse_file_by_sections, request_fp,
                          '[Request]',
                          req_keys, optional_keys)

    def test_parse_file_by_sections_fail2(self):
        """Given a file containing sections with a header.
        Header is missing error"""
        request_fp = open(
            os.path.join(BASE_DIR,
                         'fixtures/request_file_header_missing2.txt'),
            'r')
        req_keys = ['source_database_name', 'source_table_name', ]
        optional_keys = ['domain', 'db_username', 'password_file', 'jdbcurl']
        self.assertRaises(ValueError, parse_file_by_sections, request_fp,
                          '[Request]',
                          req_keys, optional_keys)

    def test_parse_file_by_sections_fail3(self):
        """request file - invalid field"""
        _path = os.path.join(
            BASE_DIR, 'fixtures/request_file_invalid_field.txt')
        request_fp = open(_path, 'r')
        req_keys = ['source_database_name', 'source_table_name', ]
        optional_keys = ['domain', 'db_username', 'password_file', 'jdbcurl']

        with self.assertRaises(ValueError) as context:
            parse_file_by_sections(
                request_fp, '[Request]', req_keys, optional_keys)
        self.assertTrue('Remove these fields: invalid_field' in
                        str(context.exception))

if __name__ == '__main__':
    unittest.main()
