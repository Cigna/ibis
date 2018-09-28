"""Tests for zookeeper_remove_locks.py"""
import unittest
import os
import logging
from mock import patch
from lib.ingest.impala_utils import ImpalaConnect, HiveConnect


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class ImpalaUtilsFunctionsTest(unittest.TestCase):
    """Tests."""

    def setUp(self):
        logging.basicConfig()

    @patch('lib.ingest.impala_utils.connect', autospec=True)
    def test_impala_run_query(self, m_connect):
        """test impala run query"""
        m_connect().cursor().fetchall.return_value = 'query_result'
        result = ImpalaConnect.run_query(
            'test_host', 'database.table', 'select count(*) from table;')
        self.assertEqual(result, 'query_result')
        result = ImpalaConnect.run_query(
            'test_host', 'database.table', 'create table db.table;',
            op='create')
        self.assertEqual(result, '')

    @patch('lib.ingest.impala_utils.connect', autospec=True)
    def test_hive_run_query(self, m_connect):
        """test hive run query"""
        m_connect().cursor().fetchall.return_value = 'query_result'
        result = HiveConnect.run_query(
            'test_host', 'database.table', 'select count(*) from table;')
        self.assertEqual(result, 'query_result')
        result = ImpalaConnect.run_query(
            'test_host', 'database.table', 'create table db.table;',
            op='create')
        self.assertEqual(result, '')


if __name__ == '__main__':
    unittest.main()
