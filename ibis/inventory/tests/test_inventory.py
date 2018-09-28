"""Tests for inventory"""
import os
import unittest

from mock import patch
from mock.mock import Mock
from ibis.inventory.inventory import Inventory
from ibis.utilities.config_manager import ConfigManager
from ibis.settings import UNIT_TEST_ENV


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class InventoryFunctionsTest(unittest.TestCase):
    """Tests the functionality of the self.inventory class"""

    def setUp(self):
        """Set Up."""
        self.inventory = Inventory(ConfigManager(UNIT_TEST_ENV))
        self.inventory._cursor = Mock()
        self.inventory.table = 'test_table'

    def tearDown(self):
        """Tear Down."""
        self.inventory = None

    @patch.object(Inventory, '_connect', autospec=True)
    def test_get_all(self, mock_connect):
        """test get all."""
        where_condition = ("source_database_name='{0}' and "
                           "source_table_name='{1}' and "
                           "db_env='{2}'")
        where_condition = where_condition.format('call', 'fake_database', 'dev')
        self.inventory.get_all(where_condition)
        self.assertTrue(self.inventory._cursor.fetchall.called)

    @patch.object(Inventory, '_connect', autospec=True)
    def test_get_metadata(self, mock_connect):
        """test get metadata."""
        self.inventory._cursor.fetchall.return_value = [('col1',), ('col2',)]
        self.assertTrue(self.inventory.get_metadata())

    @patch.object(Inventory, '_connect', autospec=True)
    def test_get_metadata_fail(self, mock_connect):
        """test get metadata with no results."""
        self.inventory._cursor.fetchall.return_value = []
        self.assertEquals([], self.inventory.get_metadata())

    @patch.object(Inventory, '_connect', autospec=True)
    def test_get_rows(self, mock_connect):
        """test fetch rows from hive."""
        self.inventory._cursor.fetchall.return_value = []
        result = self.inventory.get_rows(
            'select * from {0}'.format(self.inventory.table))
        self.assertEquals([], result)

    @patch.object(Inventory, '_connect', autospec=True)
    def test_run_query(self, mock_connect):
        """test run query."""
        self.inventory.run_query('query')
        self.assertTrue(self.inventory._cursor.execute.called)

    @patch.object(Inventory, '_connect', autospec=True)
    def test_get_table_mapping(self, mock_connect):
        """test get table mapping."""
        self.inventory._cursor.fetchall.side_effect = \
            [[('col1',), ('col2',)], [['val1', 'val2']]]
        expected = {'col1': 'val1', 'col2': 'val2'}
        result = self.inventory.get_table_mapping('call', 'fake_database', 'dev')
        print expected
        self.assertDictEqual(expected, result)

    @patch.object(Inventory, '_connect', autospec=True)
    def test_get_table_mapping_len_meta(self, mock_connect):
        """test fail for not match in len of values."""
        self.inventory._cursor.fetchall.return_value = [('col1',), ('col2',)]
        expected = {}
        result = self.inventory.get_table_mapping('call', 'fake_database', 'dev')
        self.assertDictEqual(expected, result)

    @patch.object(Inventory, '_connect', autospec=True)
    def test_get_table_mapping_no_meta(self, mock_connect):
        """test fail for non existen metadata."""
        self.inventory._cursor.fetchall.side_effect = \
            [[('col1',), ('col2',)], [[]]]
        expected = {}
        result = self.inventory.get_table_mapping('call', 'fake_database', 'dev')
        self.assertDictEqual(expected, result)

    @patch.object(Inventory, '_connect', autospec=True)
    def test_get_table_mapping_no_tbl(self, mock_connect):
        """test fail for non existent table."""
        self.inventory._cursor.fetchall.return_value = []
        expected = {}
        result = self.inventory.get_table_mapping('call', 'fake_database', 'dev')
        self.assertDictEqual(expected, result)


if __name__ == '__main__':
    unittest.main()
