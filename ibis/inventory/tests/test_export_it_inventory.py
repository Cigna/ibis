"""Tests for request inventory."""
import unittest
import os
from mock import patch, Mock, MagicMock
from ibis.inventory.export_it_inventory import ExportITInventory
from ibis.utilities.config_manager import ConfigManager
from ibis.settings import UNIT_TEST_ENV
from ibis.model.exporttable import ItTableExport

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

it_table_mock = {'full_table_name': 'test_oracle_export',
                 'source_dir': '/user/data/mdm/SQL_ordering/TEST_DB',
                 'jdbcurl': 'jdbc:sqlserver::@//fake.oracle:'
                            '1521/fake_servicename',
                 'connection_factories': 'com.quest.oraoop',
                 'password_file': '/user/dev/.password',
                 'load': '200100', 'db_username': 'fake_username', 'mappers': 10,
                 'fetch_size': 20000,
                 'target_schema': 'dbo',
                 'target_table': 'test'}


class ExportITInventoryFunctionsTest(unittest.TestCase):
    """Tests the functionality of the export it inventory class"""

    @classmethod
    @patch.object(inventory.Inventory, '_connect', autospec=True)
    def setUpClass(cls, mock_connect):
        """setup"""
        cls.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        cls.inventory = ExportITInventory(cls.cfg_mgr)

    @classmethod
    def tearDownClass(cls):
        cls.inventory = None

    def test_update_export(self):
        """ Tests update export using a mocked table """
        self.export_it_inventory = Mock(spec=ExportITInventory)
        self.export_it_inventory.return_value = [
            {'full_table_name': 'test_oracle_export',
             'source_dir': '/user/data/mdm/SQL_ordering/TEST_DB',
             'jdbcurl': 'jdbc:sqlserver:thin:@//fake.oracle:'
                        '1521/fake_servicename',
             'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
             'password_file': '/user/dev/fake.password.file',
             'load': '200100', 'db_username': 'fake_username', 'mappers': 10,
             'fetch_size': 20000,
             'target_schema': 'dbo',
             'target_table': 'test'}]
        self.assertIn('updated with db_username, target_table in \
                       export_it_inventory',
                      self.export_it_inventory.update_export('db_username',
                                                             'target_table'))

    def test_parse_requests_export(self):
        """Tests the parsing of a file containing sections of a [Request]"""
        business_req = \
            open(os.path.join(BASE_DIR,
                              'resources/mock_user_export_request.txt'), 'r')
        results, _ = self.inventory.parse_requests_export(business_req)
        self.assertTrue((results[0]).frequency_readable == 'monthly')
        self.assertTrue((results[1]).frequency_readable == 'weekly')
        self.assertTrue((results[2]).frequency_readable == 'biweekly')

    @patch.object(ExportITInventory, 'run_query', autospec=True)
    @patch.object(ExportITInventory, 'get_all', autospec=True)
    def test_insert_export(self, mock_get_all, mock_run_query):
        """Test insert."""
        table_dict = MagicMock()
        mock_get_all.side_effect = [[('test')], []]
        # row exists in table
        test_ret, _ = self.inventory.insert_export(table_dict)
        self.assertEquals(test_ret, False)

    @patch.object(ExportITInventory, 'run_query', autospec=True)
    @patch.object(ExportITInventory, 'get_all', return_value=[])
    def test_insert_export_N(self, mock_get_all, mock_run_query):
        """Test insert."""
        table_obj = ItTableExport(it_table_mock, self.cfg_mgr)
        # row doesnt exists in table
        test_ret, _ = self.inventory.insert_export(table_obj)
        self.assertEquals(test_ret, True)

    @patch.object(ExportITInventory, 'run_query', autospec=True)
    @patch.object(ExportITInventory, 'get_all', autospec=True)
    def test_update_export_Magic(self, mock_get_all, mock_run_query):
        """Test insert."""
        table_dict = MagicMock()
        mock_get_all.side_effect = [[('test')], []]
        # row doesnt exists in table
        test_ret, _ = self.inventory.update_export(table_dict)
        self.assertEquals(test_ret, True)

    @patch.object(ExportITInventory, 'run_query', autospec=True)
    @patch.object(ExportITInventory, 'get_all', return_value=[])
    def test_update_export_N(self, mock_get_all, mock_run_query):
        """Test insert."""
        table_obj = ItTableExport(it_table_mock, self.cfg_mgr)
        # row exists in table
        test_ret, _ = self.inventory.update_export(table_obj)
        self.assertEquals(test_ret, False)

if __name__ == '__main__':
    unittest.main()
