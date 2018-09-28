"""Tests for zookeeper_remove_locks.py"""
import unittest
import os
import logging
from mock import patch
from lib.ingest.import_prep import CacheTable


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class ImportPrepFunctionsTest(unittest.TestCase):
    """Tests."""

    def setUp(self):
        logging.basicConfig()
        self.cache_object = CacheTable('fake_database', 'member', 'dev', 'it_table',
                                       'host')

    def test_init(self):
        """test."""
        cache_object = CacheTable('fake_database', 'member', 'dev', 'it_table', 'host')
        self.assertEqual('fake_database', cache_object.database)
        self.assertEqual('member', cache_object.table_name)
        self.assertEqual('dev', cache_object.db_env)
        self.assertEqual('it_table', cache_object.it_table)
        self.assertEqual('host', cache_object.it_table_host)

    @patch('lib.ingest.import_prep.ImpalaConnect', autospec=True)
    def test_get_record(self, mock_source_table):
        """test."""
        mock_source_table.run_query.return_value = [('fake_database.member', 'fake_database_i',
                                                     'mdm/fake_database_i/fake_database/member',
                                                     'fake_key', 50,
                                                     'jdbc:mysql://ibis',
                                                     'connection', 'user',
                                                     'password', 000001, 50,
                                                     '0', 'FAKED0A6',
                                                     'btflmnds_scratch',
                                                     'W146', 'fake_id_column',
                                                     'dbo',
                                                     'fake_id_column > 5434',
                                                     'action', 'fake_database', 'member',
                                                     'dev')]
        self.assertEqual(self.cache_object.get_record(),
                         ('fake_database.member', 'fake_database_i',
                          'mdm/fake_database_i/fake_database/member',
                          'fake_key', 50,
                          'jdbc:mysql://ibis',
                          'connection', 'user',
                          'password', 000001, 50,
                          '0', 'FAKED0A6',
                          'btflmnds_scratch',
                          'W146', 'fake_id_column',
                          'dbo',
                          'fake_id_column > 5434',
                          'action', 'fake_database', 'member',
                          'dev'))

if __name__ == '__main__':
    unittest.main()
