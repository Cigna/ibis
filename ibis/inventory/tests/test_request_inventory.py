"""Tests for request inventory."""
import unittest
import os
from mock import patch
from ibis.inventory.inventory import Inventory
from ibis.inventory.request_inventory import RequestInventory, Request
from ibis.utilities.config_manager import ConfigManager
from ibis.settings import UNIT_TEST_ENV

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class RequestTests(unittest.TestCase):
    """test Request instances"""

    @classmethod
    def setUpClass(cls):
        """setup"""
        cls.cfg_mgr = ConfigManager(UNIT_TEST_ENV)

    @classmethod
    def tearDownClass(cls):
        pass

    def test_fields(self):
        """test fields"""
        request_dict = {
            'db_username': 'test_username',
            'password_file': 'test_passwd',
            'jdbcurl': 'test_jdbcurl',
            'domain': 'test_domain',
            'source_database_name': 'test_DB',
            'source_table_name': 'test_TABLE'
        }
        req_obj = Request(request_dict, self.cfg_mgr)
        self.assertEqual(req_obj.username, 'test_username')
        self.assertEqual(req_obj.password_file, 'test_passwd')
        self.assertEqual(req_obj.jdbcurl, 'test_jdbcurl')
        self.assertEqual(req_obj.database, 'test_db')
        self.assertEqual(req_obj.table_name, 'test_table')
        self.assertEqual(req_obj.db_table_name, 'test_db_test_table')

    def test_optional_fields(self):
        """test optional"""
        request_dict = {
            'db_username': 'test_username',
            'password_file': 'test_passwd',
            'jdbcurl': 'test_jdbcurl',
            'domain': 'test_domain',
            'source_database_name': 'test_db',
            'source_table_name': 'test_table'
        }
        req_obj = Request(request_dict, self.cfg_mgr)
        self.assertEqual(req_obj.views, False)
        self.assertEqual(req_obj.frequency_readable, False)
        self.assertEqual(req_obj.esp_group, False)


class RequestInventoryFunctionsTest(unittest.TestCase):
    """Tests the functionality of the request inventory class"""

    @classmethod
    @patch.object(Inventory, '_connect', autospec=True)
    def setUpClass(cls, mock_connect):
        """setup"""
        cls.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        cls.inventory = RequestInventory(cls.cfg_mgr)

    @classmethod
    def tearDownClass(cls):
        cls.inventory = None

    def test_parse_file(self):
        """Tests the parsing of a file containing sections of a [Request]"""
        business_req = open(os.path.join(BASE_DIR,
                                         'resources/mock_user_request.txt'),
                            'r')
        results, _ = self.inventory.parse_file(business_req)
        self.assertTrue((results[0]).frequency_readable == 'monthly')
        self.assertTrue((results[1]).frequency_readable == 'weekly')
        self.assertTrue((results[2]).frequency_readable == 'biweekly')

    @patch.object(Inventory, 'get_table_mapping', autospec=True)
    def test_get_available_requests(self, mock_get_table_mapping):
        """Tests the retrieval of available and unavailable
        for workflow generation"""
        valid_req = {'db_username': 'fake_username', 'password_file': 'test',
                     'jdbcurl': 'test', 'domain': 'rx',
                     'source_database_name': 'fake_database',
                     'source_table_name': 'clm_fact',
                     'refresh_frequency': 'monthly'}
        unavailable_req = {'db_username': 'fake_username', 'password_file': 'test',
                           'jdbcurl': 'test', 'domain': 'rx',
                           'source_database_name': 'invalid_req',
                           'source_table_name': 'invalid_req',
                           'refresh_frequency': 'weekly'}
        onhold_req = {'db_username': 'fake_username', 'password_file': 'test',
                      'jdbcurl': 'test', 'domain': 'rx',
                      'source_database_name': 'onhold_req',
                      'source_table_name': 'onhold_req',
                      'refresh_frequency': 'weekly'}
        reqs = [Request(valid_req, self.cfg_mgr),
                Request(onhold_req, self.cfg_mgr),
                Request(unavailable_req, self.cfg_mgr)]
        mock_ret_val = {
            'load': '000010', 'mappers': 10, 'domain': 'member',
            'source_database_name': 'fake_database',
            'target_dir': 'mdm/member/fake_database/clm_fact',
            'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
            'password_file': 'test',
            'full_table_name': 'member.fake_database_clm_fact',
            'source_table_name': 'clm_fact',
            'db_username': 'fake_username', 'hold': 0, 'split_by': '',
            'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
                       'fake_servicename',
            'fetch_size': 50000}
        mock_ret_val_2 = {
            'load': '000010', 'mappers': 10, 'domain': 'member',
            'source_database_name': 'invalid_db',
            'target_dir': 'mdm/member/fake_database/clm_fact',
            'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
            'password_file': 'test',
            'full_table_name': 'member.fake_database_clm_fact',
            'source_table_name': 'invalid_table',
            'db_username': 'fake_username', 'hold': 1, 'split_by': '',
            'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
                       'fake_servicename',
            'fetch_size': 50000}
        mock_get_table_mapping.side_effect = [mock_ret_val, mock_ret_val_2, {}]
        available_tables, onhold_tables, unavailable_requests = \
            self.inventory.get_available_requests(reqs)
        self.assertEqual((available_tables[0]).database, 'fake_database')
        self.assertEqual((onhold_tables[0]).database, 'invalid_db')
        self.assertEqual((unavailable_requests[0]).database, 'invalid_req')

    @patch.object(Inventory, 'get_table_mapping', autospec=True)
    def test_get_available_requests_export(self, mock_get_table_mapping):
        """Tests the retrieval of available and unavailable for
        export workflow generation"""
        valid_req = {'db_username': 'valid_user', 'password_file': 'test',
                     'jdbcurl': 'test', 'domain': 'valid_member',
                     'db_env': 'dev',
                     'source_database_name': 'db_valid',
                     'source_table_name': 'QA_test_valid',
                     'refresh_frequency': 'weekly'}
        unavailable_req = {'db_username': 'invalid_user',
                           'password_file': 'test',
                           'jdbcurl': 'test', 'domain': 'invalid_membe',
                           'source_database_name': 'db_invalid',
                           'source_table_name': 'QA_test_invalid',
                           'refresh_frequency': 'weekly'}

        reqs = [Request(valid_req, self.cfg_mgr),
                Request(unavailable_req, self.cfg_mgr)]

        mock_ret_val = {
            'load': '33', 'mappers': 1, 'domain': 'valid_member',
            'source_database_name': 'db_valid'}

        mock_get_table_mapping.side_effect = [mock_ret_val, {}]

        available_tables, unavailable_requests =\
            self.inventory.get_available_requests_export(reqs)
        self.assertEqual((available_tables[0]).database, 'db_valid')
        self.assertEqual(unavailable_requests[0].database, 'db_invalid')

        """To test the condition without unavailable_requests"""
        table_req = {'db_username': 'valid_user', 'password_file': 'test',
                     'jdbcurl': 'test', 'domain': 'valid_member',
                     'db_env': 'dev',
                     'source_database_name': 'db_valid',
                     'source_table_name': 'QA_test_valid',
                     'refresh_frequency': 'weekly'}
        reqs = [Request(table_req, self.cfg_mgr)]
        mock_tab_ret_val = {'load': '44', 'mappers': 3,
                            'domain': 'valid_table',
                            'source_database_name': 'db_valid'}
        mock_get_table_mapping.side_effect = [mock_tab_ret_val]
        available_tables, unavailable_requests =\
            self.inventory.get_available_requests_export(reqs)
        self.assertEqual((available_tables[0]).database, 'db_valid')


if __name__ == '__main__':
    unittest.main()
else:
    loader = unittest.TestLoader()

    test_classes_to_run = [RequestInventoryFunctionsTest, RequestTests]
    # Create test suite
    request_inventory_test_suite = []
    for test_class in test_classes_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        request_inventory_test_suite.append(suite)
