"""ESP IDS inventory tests."""
import unittest
import os
import difflib
from mock.mock import patch
from ibis.inventory.inventory import Inventory
from ibis.utilities.config_manager import ConfigManager
from ibis.inventory.esp_ids_inventory import ESPInventory
from ibis.settings import UNIT_TEST_ENV
from ibis.model.table import ItTable
from ibis.inventor.tests.fixture_workflow_generator import \
    fake_fact_tbl_prop, fake_ben_tbl_prop, fake_prof_tbl_prop

appl_ref_sch = [('FAKED001', 'C1_FAKE_FAKE_DATABASE_DAILY', '3:00', 'Every Day',
                 'FAKE_DATABASE_DAILY', 'FAKE', 'call', 'FAKE_DATABASE',
                 'saily', '', 'DEV')]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class ESPInventoryFunctionsTest(unittest.TestCase):
    """Test the functionality of the it inventory class.
    Tests against the fake_clm_tablename record.
    """

    @classmethod
    @patch.object(Inventory, '_connect', autospec=True)
    def setUpClass(cls, mock_connect):
        """Setup."""
        cls.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        cls.inventory = ESPInventory(cls.cfg_mgr)

    @classmethod
    def tearDownClass(cls):
        cls.inventory = None

    def files_equal(self, test_file, expected_file):
        """Compares two files"""
        same = True
        test_fh = open(test_file, 'r')
        fo_gen = open(expected_file, 'r')
        test_str = test_fh.read()
        expected_str = fo_gen.read()
        if not self.strings_equal(test_str, expected_str):
            print "Generated test file:{0}".format(test_file)
            print "Fix the file:{0}".format(expected_file)
            same = False

        test_fh.close()
        fo_gen.close()
        return same

    def strings_equal(self, test_str, expected_str):
        """compare strings"""
        same = True
        test_str = [xml.strip().replace('\t', '') for xml in
                    test_str.splitlines()]
        expected_str = [xml.strip().replace('\t', '') for xml in
                        expected_str.splitlines()]
        if "".join(expected_str) != "".join(test_str):
            same = False
            print '\n'
            print '=' * 100
            print "\nFiles don't match..."
            diff = difflib.unified_diff(expected_str, test_str)
            print '\n'.join(list(diff))
        return same

    @patch.object(ESPInventory, 'get_rows', return_value=appl_ref_sch)
    def test_get_tables_by_id(self, mock_rows):
        """Test get tables by id with results"""
        expected = [{'domain': 'call', 'string_date': 'Every Day',
                     'db': 'FAKE_DATABASE', 'ksh_name': 'FAKE_DATABASE_DAILY',
                     'frequency': 'saily', 'esp_domain': 'FAKE',
                     'esp_group': '', 'environment': 'DEV',
                     'appl_id': 'FAKED001', 'time': '3:00',
                     'job_name': 'C1_FAKE_FAKE_DATABASE_DAILY'}]
        self.assertListEqual(expected,
                             self.inventory.get_tables_by_id('FAKED001'))

    @patch.object(ESPInventory, 'get_rows', return_value=[])
    def test_get_tables_by_id_without_results(self, mock_rows):
        """Test get tables by id with no results"""
        expected = []
        self.assertListEqual(expected,
                             self.inventory.get_tables_by_id('FAKED001'))

    @patch.object(ESPInventory, 'get_tables_by_id', return_value=False)
    @patch.object(ESPInventory, 'run_query', autospec=True)
    def test_insert(self, mock_get, mock_run):
        """Test insert into esp_ids table"""
        mock_tbl = {'domain': 'call', 'string_date': 'Every Day',
                    'db': 'FAKE_DATABASE', 'ksh_name': 'FAKE_DATABASE_DAILY',
                    'frequency': 'saily', 'esp_domain': 'FAKE',
                    'esp_group': '', 'environment': 'DEV',
                    'appl_id': 'FAKED001', 'time': '3:00',
                    'job_name': 'C1_FAKE_FAKE_DATABASE_DAILY'}
        result, msg = self.inventory.insert(mock_tbl)
        self.assertTrue(result)
        self.assertEquals(
            msg, 'Inserted new record FAKED001 into ibis.esp_ids table')

    @patch.object(ESPInventory, 'get_tables_by_id', return_value=True)
    @patch.object(ESPInventory, 'run_query', autospec=True)
    def test_insert_fail(self, mock_get, mock_run):
        """Test insert into esp_ids table failed"""
        mock_tbl = {
            'domain': 'call', 'string_date': 'Every Day', 'db': 'FAKE_DATABASE',
            'ksh_name': 'FAKE_DATABASE_DAILY', 'frequency': 'saily',
            'esp_domain': 'FAKE', 'esp_group': '', 'environment': 'DEV',
            'appl_id': 'FAKED001', 'time': '3:00',
            'job_name': 'C1_FAKE_FAKE_DATABASE_DAILY'}
        result, msg = self.inventory.insert(mock_tbl)
        self.assertFalse(result)
        self.assertEquals(
            msg,
            'ID FAKED001 NOT INSERTED into ibis.esp_ids. Already exists!')

    # Assumption = Previous appl ids 'FAKEW231','FAKEW261','FAKED211'
    @patch.object(ESPInventory, 'get_rows', autospec=True)
    @patch.object(ESPInventory, 'insert', return_value=(True, 'Success'))
    def test_gen_esp_id(self, mock_insert, mock_rows):
        mock_rows.side_effect = [[], [('M26',)]]
        test_esp_id = self.inventory.gen_esp_id('MontHly', 'ClAim',
                                                'fake_view', 'Magic')
        self.assertEqual('FAKEM271', test_esp_id)

    # Assumption = Previous appl ids 'FAKEW001', 'FAKEW181', 'FAKED161'
    @patch.object(ESPInventory, 'get_rows', autospec=True)
    @patch.object(ESPInventory, 'insert', return_value=(True, 'Success'))
    def test_gen_esp_id_member(self, mock_insert, mock_rows):
        mock_rows.side_effect = [[], [('W18',)]]
        self.assertEqual('FAKEW191', self.inventory.gen_esp_id(
            'Weekly', 'Member', 'fake_database'))

    @patch.object(ESPInventory, 'get_rows', return_value=[])
    @patch.object(ESPInventory, 'insert', return_value=(True, 'Success'))
    def test_gen_esp_id_no_results(self, mock_insert, mock_rows):
        self.assertEqual('FAKEW001', self.inventory.gen_esp_id(
            'Weekly', 'Member', 'fake_database'))

    @patch.object(ESPInventory, 'get_rows', return_value=[])
    def test_calculate_esp_id(self, mock_rows):
        """Test calculate new esp id"""
        self.assertEquals(self.inventory.calculate_esp_id('Daily'), 'FAKED001')

    @patch.object(ESPInventory, 'get_rows', return_value=[('D00',), ('D01',)])
    def test_calculate_esp_id_next_seq(self, mock_rows):
        """Test calculate next esp id"""
        self.assertEquals(self.inventory.calculate_esp_id('Daily'), 'FAKED021')

    @patch.object(ESPInventory, 'get_rows', return_value=[('D14',), ('D2z',)])
    def test_calculate_esp_id_z(self, mock_rows):
        """Test calculate next esp id"""
        self.assertEquals(self.inventory.calculate_esp_id('Daily'), 'FAKED301')

    def test_gen_wld_subworkflow(self):
        """test wld for subworkflows"""
        appl_ref_id_tbl = {
            'job_name': 'C1_FAKE_CALL_FAKE_DATABASE_DAILY',
            'frequency': 'Daily', 'time': '6:00', 'string_date': 'Every Day',
            'ksh_name': 'call_fake_database_daily', 'domain': 'call',
            'db': 'fake_database', 'environment': 'DEV'}
        self.inventory.gen_wld_subworkflow('FAKED306', appl_ref_id_tbl, 5)
        test = os.path.join(self.inventory.cfg_mgr.files,
                            'FAKED306_subworkflow.wld')
        expected = os.path.join(BASE_DIR, 'expected/test_subworkflow.wld')
        bool_val = self.files_equal(test, expected)
        self.assertTrue(bool_val)

    def test_gen_wld_tables(self):
        """test wld for table workflow"""
        table1 = ItTable(fake_fact_tbl_prop, self.cfg_mgr)
        table2 = ItTable(fake_ben_tbl_prop, self.cfg_mgr)
        table3 = ItTable(fake_prof_tbl_prop, self.cfg_mgr)
        tables = [table1, table2, table3]
        workflow_names = [table1.db_table_name, table2.db_table_name,
                          table3.db_table_name]
        self.inventory.gen_wld_tables('FAKED306', tables, workflow_names)
        test = os.path.join(self.inventory.cfg_mgr.files, 'FAKED306.wld')
        expected = os.path.join(BASE_DIR, 'expected/test_tables.wld')
        bool_val = self.files_equal(test, expected)
        self.assertTrue(bool_val)

if __name__ == "__main__":
    unittest.main()
