"""It inventory tests."""
import unittest
import os
import sys
from mock import patch, MagicMock
from ibis.inventory.inventory import Inventory
from ibis.inventory.it_inventory import ITInventory
from ibis.utilities.config_manager import ConfigManager
from ibis.model.table import ItTable
from ibis.settings import UNIT_TEST_ENV


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

mock_claim_tbl = [[
    'claim.fake_database_fake_clm_tablename', 'claim',
    'mdm/claim/fake_database/fake_clm_tablename', 'fake_split_by', '20',
    'jdbc:teradata://fake.teradata/DATABASE=fake_database',
    'com.cloudera.connector.teradata.TeradataManagerFactory',
    'fake_username',
    'jceks://hdfs/user/dev/fake.passwords.jceks#fake.password.alias',
    '010001', '50000', '0', 'TEST01', 'fake_view_im|fake_view_open', '', 'test_inc_column',
    'dbo', 'TRANS > 40', '', 'fake_database', 'fake_clm_tablename', 'sys']]
mock_claim_tbl_dict = [{
    'full_table_name': 'claim.fake_database_fake_clm_tablename',
    'domain': 'claim',
    'target_dir': 'mdm/claim/fake_database/fake_clm_tablename',
    'split_by': 'fake_split_by', 'mappers': '20',
    'jdbcurl': 'jdbc:teradata://fake.teradata/'
               'DATABASE=fake_database',
    'connection_factories': 'com.cloudera.connector.teradata.'
                            'TeradataManagerFactory',
    'db_username': 'fake_username',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                     'fake.password.alias',
    'load': '010001', 'fetch_size': '50000', 'hold': '0',
    'esp_appl_id': 'TEST01', 'views': 'fake_view_im|fake_view_open', 'esp_group': '',
    'check_column': 'test_inc_column', 'source_schema_name': 'dbo',
    'sql_query': 'TRANS > 40', 'actions': '', 'db_env': 'sys',
    'source_database_name': 'fake_database',
    'source_table_name': 'fake_clm_tablename'}]


class ITInventoryFunctionsTest(unittest.TestCase):
    """Test the functionality of the it inventory class.
    Tests against the fake_clm_tablename record."""

    @classmethod
    @patch.object(Inventory, '_connect', autospec=True)
    def setUpClass(cls, mock_connect):
        """Setup."""
        cls.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        cls.inventory = ITInventory(cls.cfg_mgr)

    @classmethod
    def tearDownClass(cls):
        cls.inventory = None

    @patch.object(ITInventory, 'insert', autospec=True)
    def test_insert_placeholder(self, mock_insert):
        """Test that a placeholder record gets inserted into the it table.
        With a hold of 1."""
        ret = self.inventory.insert_placeholder('test_db', 'test_table_name')
        self.assertEquals(ret, None)

    @patch.object(ITInventory, 'run_query', autospec=True)
    def test_drop_partition(self, mock_run_query):
        """Test drop partition."""
        ret = self.inventory.drop_partition('test_db', 'test_table_name')
        self.assertEquals(ret, None)

    @patch.object(ITInventory, 'run_query', autospec=True)
    @patch.object(ITInventory, 'get_all', autospec=True)
    def test_insert(self, mock_get_all, mock_run_query):
        """Test insert."""
        table_dict = MagicMock()
        mock_get_all.side_effect = [[('test')], []]
        # row exists in table
        test_ret, _ = self.inventory.insert(table_dict)
        self.assertEquals(test_ret, False)
        # row doesnt exists in table
        test_ret, _ = self.inventory.insert(table_dict)
        self.assertEquals(test_ret, True)

    @patch.object(ITInventory, 'run_query', autospec=True)
    @patch.object(ITInventory, 'get_all', autospec=True)
    def test_update(self, mock_get_all, mock_run_query):
        """Test insert."""
        table_dict = MagicMock()
        mock_get_all.side_effect = [[('test')], []]
        # row exists in table
        test_ret, _ = self.inventory.update(table_dict)
        self.assertEquals(test_ret, True)
        # row doesnt exists in table
        test_ret, _ = self.inventory.update(table_dict)
        self.assertEquals(test_ret, False)

    @patch.object(ITInventory, 'get_rows', autospec=True)
    def test_get_all_tables(self, mock_rows):
        """test get all tables"""
        mock_rows.return_value = mock_claim_tbl
        result = self.inventory.get_all_tables()
        self.assertEquals(result, mock_claim_tbl_dict)

    @patch.object(os, 'open', autospec=True)
    @patch.object(ITInventory, 'get_all_tables', autospec=True)
    def test_save_all_tables(self, mock_all_tables, mock_open):
        """test save all tables"""
        mock_all_tables.return_value = mock_claim_tbl_dict
        mck_src_type = "teradata"
        mck_invalid_src_type = "sqlserver"
        mck_file_name = "it_table_2017-01-22_22:11:33"
        expected = "Saved 1 tables in ibis.dev_it_table"
        expected_invalid = "Saved 0 tables in ibis.dev_it_table"

        result = self.inventory.save_all_tables(mck_file_name, mck_src_type)
        result = result[0:35]
        self.assertEquals(result, expected)
        result = self.inventory.save_all_tables(mck_file_name,
                                                mck_invalid_src_type)
        result_invalid = result[0:35]
        self.assertEquals(result_invalid, expected_invalid)

    @patch.object(ITInventory, 'get_rows', return_value=[])
    def test_get_all_tables_no_tbls(self, mock_rows):
        """test get all tables"""
        result = self.inventory.get_all_tables()
        self.assertEquals([], result)

    @patch.object(ITInventory, 'get_rows',
                  side_effect=[[('call',), ('domain1',)], []])
    def test_get_domains(self, mock_rows):
        self.assertEquals(self.inventory.get_domains(), ['call', 'domain1'])
        self.assertEquals(self.inventory.get_domains(), [])

    @patch.object(ITInventory, 'get_rows', autospec=True)
    def test_get_all_tables_for_esp(self, mock_rows):
        mock_rows.side_effect = [mock_claim_tbl, []]
        result = self.inventory.get_all_tables_for_esp('TEST01')
        table_obj = ItTable(mock_claim_tbl_dict[0], self.cfg_mgr)
        self.assertEquals(result[0], table_obj)
        result = self.inventory.get_all_tables_for_esp('TEST02')
        self.assertEquals(result, [])

    @patch.object(ITInventory, 'get_rows', autospec=True)
    def test_get_all_tables_for_domain(self, mock_rows):
        mock_rows.side_effect = [mock_claim_tbl, []]
        result = self.inventory.get_all_tables_for_domain('claim')
        self.assertEquals(result, mock_claim_tbl_dict)
        result = self.inventory.get_all_tables_for_domain('domain1')
        self.assertEquals(result, [])

    @patch.object(ITInventory, 'get_rows', autospec=True)
    def test_get_all_tables_for_schedule(self, mock_rows):
        mock_rows.side_effect = [mock_claim_tbl, []]
        result = self.inventory.get_all_tables_for_schedule('monthly')
        self.assertEquals(result, mock_claim_tbl_dict)
        result = self.inventory.get_all_tables_for_schedule('020')
        self.assertEquals(result, [])

    @patch.object(sys, 'exit', autospec=True)
    @patch.object(ITInventory, 'get_rows', return_value=mock_claim_tbl)
    def test_get_all_tables_for_schedule_fail(self, mock_rows, mock_sys):
        """test error handler."""
        tbl_fail = mock_claim_tbl
        tbl_fail[0][9] = '0'
        result = self.inventory.get_all_tables_for_schedule('monthly')
        self.assertEquals(result, [])


if __name__ == '__main__':
    unittest.main()
