"""IT table generation tests."""

import unittest
import os
from mock import patch, MagicMock
from ibis.utilities.it_table_generation import SourceTable, OracleTable, \
    DB2Table, TeradataTable, SqlServerTable, MySQLTable, create, \
    get_auto_values, get_src_obj, parallel_sqoop_output, Get_Auto_Split
from ibis.model.table import ItTable
from ibis.utilities.config_manager import ConfigManager
from ibis.settings import UNIT_TEST_ENV
from ibis.inventory.it_inventory import ITInventory


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class SourceTableTest(unittest.TestCase):
    """Test SourceTable methods."""

    def setUp(self):
        """Setup."""
        self.it_table = {
            'source_database_name': 'fake_database',
            'source_table_name': 'fake_cen_tablename',
            'db_username': 'fake_username',
            'password_file': 'test-passwd',
            'connection_factories': 'test-conn-factory',
            'jdbcurl': 'oracle:jdbcurl',
            'mappers': '5'
        }
        self.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        self.it_table_obj = ItTable(self.it_table, self.cfg_mgr)
        self.src_obj = SourceTable(self.cfg_mgr, self.it_table_obj)

    def tearDown(self):
        """Tear down."""
        self.src_obj._close_logs()

    def test_find_load(self):
        """Test load."""
        self.assertEquals(self.src_obj.find_load(1000000), '100')
        self.assertEquals(self.src_obj.find_load(10000000), '010')
        self.assertEquals(self.src_obj.find_load(100000002), '001')

    def test_find_mappers(self):
        """Test mappers."""
        self.assertEquals(self.src_obj.find_mappers(250001), '5')
        self.assertEquals(self.src_obj.find_mappers(249999), '1')
        self.assertEquals(self.src_obj.find_mappers(250001, '010'), '5')
        self.assertEquals(self.src_obj.find_mappers(249999, '100'), '1')

    def test_build_group_by_count_query(self):
        """Test group by query string."""
        result = self.src_obj.build_group_by_count_query('test_column')
        expected = ("SELECT COUNT(test_column) FROM "
                    "FAKE_DATABASE.FAKE_CEN_TABLENAME GROUP BY test_column")
        self.assertEquals(result, expected)

    @patch.object(SourceTable, 'find_mappers', autospec=True)
    @patch.object(SourceTable, 'find_split_by_column', autospec=True)
    @patch.object(SourceTable, 'find_load', autospec=True)
    @patch.object(SourceTable, 'get_table_count', autospec=True)
    def test_generate_it_table(self, mock_get_table_count, mock_find_load,
                               mock_find_split_by_column, mock_find_mappers):
        """Test generate it table."""
        mock_get_table_count.return_value = 1000
        mock_find_load.return_value = '100'
        mock_find_split_by_column.side_effect = ['', 'TEST_COLUMN']
        status, _ = self.src_obj.generate_it_table(45)
        self.assertEquals(status, 'manual')
        status, _ = self.src_obj.generate_it_table(45)
        self.assertEquals(status, 'auto')

    @patch('subprocess.Popen')
    def test_get_table_count(self, mock_sub_Popen):
        """Test generate it table."""
        proc = MagicMock()
        proc.communicate = MagicMock()
        proc.communicate.return_value = ('test_output', 'sqoop error')
        proc.returncode.return_value = 100
        mock_sub_Popen.return_value = proc
        with self.assertRaises(ValueError) as cm:
            self.src_obj.get_table_count()
        self.assertEquals(cm.exception.message, 'sqoop error')

    @patch.object(SourceTable, 'eval', autospec=True)
    def test_get_table_count_fetch(self, mock_eval):
        """Test generate it table."""
        with open(BASE_DIR + '/fixtures/eval_mock.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()
        mock_eval.return_value = (0, sqoop_eval_output, '')
        row_count = self.src_obj.get_table_count()
        self.assertEquals(row_count, 3)

    def test_fetch_rows_sqoop(self):
        """Test generate it table."""
        with open(BASE_DIR + '/fixtures/eval_with_column.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()
        column_labels, row_values = \
            self.src_obj.fetch_rows_sqoop(sqoop_eval_output,
                                          True, [1])
        self.assertEquals(column_labels, ["name"])
        self.assertEquals(row_values, [['Matt'], ['Mani'], ['Shiva']])

    @patch.object(SourceTable, 'eval', autospec=True)
    def test_parallel_sqoop_output(self, mock_eval):
        """Test generate it table."""
        with open(BASE_DIR + '/fixtures/eval_mock.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()
        mock_eval.return_value = (0, sqoop_eval_output, '')
        info = [self.cfg_mgr, self.it_table,
                'column_name', 'select column_name from table ']
        row_count = parallel_sqoop_output(info)
        self.assertEquals(row_count, [('column_name', 0.0, 1)])


class OracleTableTest(unittest.TestCase):
    """Test OracleTable methods."""

    def setUp(self):
        """Setup."""
        it_table = {
            'source_database_name': 'fake_database',
            'source_table_name': 'fake_cen_tablename',
            'db_username': 'fake_username',
            'password_file': 'test-passwd',
            'connection_factories': 'test-conn-factory',
            'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle'
                       ':1521/fake_servicename',
            'mappers': '10'
        }
        self.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        it_table_obj = ItTable(it_table, self.cfg_mgr)
        self.ora_obj = OracleTable(self.cfg_mgr, it_table_obj)

    def tearDown(self):
        """Tear down."""
        self.ora_obj._close_logs()

    def test_find_mappers(self):
        """Find mappers."""
        result = self.ora_obj.find_mappers(250001)
        self.assertEquals(result, '10')
        result = self.ora_obj.find_mappers(2500)
        self.assertEquals(result, '2')
        result = self.ora_obj.find_mappers(250001, '010')
        self.assertEquals(result, '10')
        result = self.ora_obj.find_mappers(2500, '100')
        self.assertEquals(result, '2')

    def test_find_primary_key(self):
        """Find pk."""
        result = self.ora_obj.find_primary_key()
        self.assertEquals(result, None)

    def test_find_split_by_column(self):
        """Find split by column."""
        result = self.ora_obj.find_split_by_column('5')
        self.assertEquals(result, "")


class DB2TableTest(unittest.TestCase):
    """Test DB2Table methods."""

    def setUp(self):
        """Setup."""
        it_table = {
            'source_database_name': 'fake_database',
            'source_table_name': 'fake_cen_tablename',
            'db_username': 'fake_username',
            'password_file': 'test-passwd',
            'connection_factories': 'test-conn-factory',
            'jdbcurl': 'jdbc:db2:',
            'mappers': '10'
        }
        self.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        it_table_obj = ItTable(it_table, self.cfg_mgr)
        self.db2_obj = DB2Table(self.cfg_mgr, it_table_obj)
        self.mock_claim_tbl_dict_DB2 = [{
            'full_table_name': 'claim.fake_database_fake_clm_tablename',
            'domain': 'claim',
            'target_dir': 'mdm/claim/fake_database/fake_clm_tablename',
            'split_by': 'fake_split_by', 'mappers': '20',
            'jdbcurl': 'jdbc:db2://fake.db2:50200/fake_servicename',
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
            'source_table_name': 'admission'}]

    @patch.object(SourceTable, 'eval', autospec=True)
    def test_find_split_by_column_found(self, mock_eval):
        """Test generate it table."""
        with open(BASE_DIR + '/fixtures/eval_mock.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()
        mock_eval.side_effect = [(0, sqoop_eval_output, '')]
        split_by_column = self.db2_obj.find_split_by_column(2)
        self.assertEqual(split_by_column, '3')

    @patch.object(SourceTable, 'get_groupby_counts', autospec=True)
    @patch.object(SourceTable, 'eval', autospec=True)
    def test_find_split_by_column_no(self, mock_eval, mock_group):
        """Test generate it table."""
        with open(BASE_DIR + '/fixtures/eval_empty.txt',
                  'r') as file_h:
            sqoop_eval_nooutput = file_h.read()
        with open(BASE_DIR + '/fixtures/eval_mock.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()
        mock_eval.side_effect = [(0, sqoop_eval_nooutput, ''),
                                 (0, sqoop_eval_output, '')]
        mock_group.return_value = ['key1', 'key2']
        split_by_column = self.db2_obj.find_split_by_column(2)
        self.assertEqual(split_by_column, 'k')

    @patch.object(SourceTable, 'eval', autospec=True)
    def test_get_split_by_DB2_pk(self, mock1):
        # Mock table found DB2
        with open(BASE_DIR + '/fixtures/db2_table_eval_mock.txt',
                  'r') as file_h:
            sqoop_eval_output1 = file_h.read()
        # Mock primary key found
        with open(BASE_DIR + '/fixtures/db2_primary_eval_mock.txt',
                  'r') as file_h:
            sqoop_eval_output2 = file_h.read()
        mock1.side_effect = [(0, sqoop_eval_output1, ''),
                             (0, sqoop_eval_output2, '')]
        table_obj = ItTable(self.mock_claim_tbl_dict_DB2[0], self.cfg_mgr)
        split_by_obj = Get_Auto_Split(self.cfg_mgr)
        result = split_by_obj.get_split_by_column(table_obj)
        print "Result: ", result
        self.assertEquals(result, "KEY")

    @patch.object(SourceTable, 'eval', autospec=True)
    def test_get_split_by_DB2_int(self, mock1):
        # Mock table found DB2
        with open(BASE_DIR + '/fixtures/db2_table_eval_mock.txt',
                  'r') as file_h:
            sqoop_eval_output1 = file_h.read()
        # Mock no primary key
        with open(BASE_DIR + '/fixtures/eval_empty.txt',
                  'r') as file_h:
            sqoop_eval_output2 = file_h.read()
        # Mock integer columns
        with open(BASE_DIR + '/fixtures/db2_primary_eval_mock.txt',
                  'r') as file_h:
            sqoop_eval_output3 = file_h.read()
        # Mock integer column in index found
        with open(BASE_DIR + '/fixtures/db2_uniqidx_eval_mock.txt',
                  'r') as file_h:
            sqoop_eval_output4 = file_h.read()
        mock1.side_effect = [(0, sqoop_eval_output1, ''),
                             (0, sqoop_eval_output2, ''),
                             (0, sqoop_eval_output3, ''),
                             (0, sqoop_eval_output4, '')]
        table_obj = ItTable(self.mock_claim_tbl_dict_DB2[0], self.cfg_mgr)
        split_by_obj = Get_Auto_Split(self.cfg_mgr)
        result = split_by_obj.get_split_by_column(table_obj)
        print "Result: ", result
        self.assertEquals(result, "KEY")

    @patch.object(SourceTable, 'eval', autospec=True)
    def test_get_split_by_DB2_noTable(self, mock1):
        # Mock table not found DB2
        with open(BASE_DIR + '/fixtures/eval_empty.txt',
                  'r') as file_h:
            sqoop_eval_output1 = file_h.read()
            sqoop_eval_output2 = sqoop_eval_output1
        # Mock integer columns
        with open(BASE_DIR + '/fixtures/db2_uniqidx_eval_mock.txt',
                  'r') as file_h:
            sqoop_eval_output3 = file_h.read()
        mock1.side_effect = [(0, sqoop_eval_output1, ''),
                             (0, sqoop_eval_output2, ''),
                             (0, sqoop_eval_output3, '')]
        table_obj = ItTable(self.mock_claim_tbl_dict_DB2[0], self.cfg_mgr)
        split_by_obj = Get_Auto_Split(self.cfg_mgr)
        result = split_by_obj.get_split_by_column(table_obj)
        print "Result: ", result
        self.assertEquals(result, "KEY")

    @patch.object(SourceTable, 'eval', autospec=True)
    def test_get_split_by_DB2_errEvalTable_IntCol(self, mock1):
        # Mock table found
        with open(BASE_DIR + '/fixtures/db2_err_eval_mock.txt',
                  'r') as file_h:
            sqoop_eval_output1 = file_h.read()
        # Mock table found uDB2
        with open(BASE_DIR + '/fixtures/db2_table_eval_mock.txt',
                  'r') as file_h:
            sqoop_eval_output2 = file_h.read()
        # Mock integer columns
        with open(BASE_DIR + '/fixtures/db2_uniqidx_eval_mock.txt',
                  'r') as file_h:
            sqoop_eval_output3 = file_h.read()
        mock1.side_effect = [(0, sqoop_eval_output1, ''),
                             (0, sqoop_eval_output2, ''),
                             (0, sqoop_eval_output3, '')]
        table_obj = ItTable(self.mock_claim_tbl_dict_DB2[0], self.cfg_mgr)
        split_by_obj = Get_Auto_Split(self.cfg_mgr)
        result = split_by_obj.get_split_by_column(table_obj)
        print "Result: ", result
        self.assertEquals(result, "KEY")

    @patch.object(SourceTable, 'eval', autospec=True)
    def test_get_split_by_DB2_errEvalTable_NoIntCol(self, mock1):
        # Mock table found
        with open(BASE_DIR + '/fixtures/db2_err_eval_mock.txt',
                  'r') as file_h:
            sqoop_eval_output1 = file_h.read()
        # Mock table found uDB2
        with open(BASE_DIR + '/fixtures/db2_err_eval_mock.txt',
                  'r') as file_h:
            sqoop_eval_output2 = file_h.read()
        # Mock integer columns
        with open(BASE_DIR + '/fixtures/db2_err_eval_mock.txt',
                  'r') as file_h:
            sqoop_eval_output3 = file_h.read()
        mock1.side_effect = [(0, sqoop_eval_output1, ''),
                             (0, sqoop_eval_output2, ''),
                             (0, sqoop_eval_output3, '')]
        table_obj = ItTable(self.mock_claim_tbl_dict_DB2[0], self.cfg_mgr)
        split_by_obj = Get_Auto_Split(self.cfg_mgr)
        result = split_by_obj.get_split_by_column(table_obj)
        print "Result: ", result
        self.assertEquals(result, "no-split")

    def tearDown(self):
        """Tear down."""
        self.db2_obj._close_logs()


class MySQLTableTest(unittest.TestCase):
    """Test DB2Table methods."""

    def setUp(self):
        """Setup."""
        it_table = {
            'source_database_name': 'fake_database',
            'source_table_name': 'fake_cen_tablename',
            'db_username': 'fake_username',
            'password_file': 'test-passwd',
            'connection_factories': 'test-conn-factory',
            'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle'
                       ':1521/fake_servicename',
            'mappers': '10'
        }
        self.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        it_table_obj = ItTable(it_table, self.cfg_mgr)
        self.mysql_obj = MySQLTable(self.cfg_mgr, it_table_obj)
        self.mock_claim_tbl_dict_mysql = [{
            'full_table_name': 'claim.fake_database_fake_clm_tablename',
            'domain': 'claim',
            'target_dir': 'mdm/claim/fake_database/fake_clm_tablename',
            'split_by': 'fake_split_by', 'mappers': '20',
            'jdbcurl': 'jdbc:mysql://fake.teradata/'
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

    @patch("ibis.utilities.it_table_generation.SourceTable._find_primary_key",
           autospec=True)
    def test_find_primary_key(self, mock1):
        result = self.mysql_obj.find_primary_key()
        self.assertEquals(result, None)

    def test_build_primary_key_query(self):
        result = self.mysql_obj.build_primary_key_query()
        expected = ("SELECT kcu.column_name FROM "
                    "information_schema.key_column_usage"
                    " kcu WHERE table_schema = 'FAKE_DATABASE' AND "
                    "constraint_name = 'PRIMARY' AND table_name = 'FAKE_CEN_TABLENAME'")
        self.assertEquals(result, expected)

    def test_build_column_type_query(self):
        result = self.mysql_obj.build_column_type_query()
        expected = ("SELECT COLUMN_NAME, DATA_TYPE FROM "
                    "INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG"
                    "='FAKE_DATABASE' and TABLE_NAME='FAKE_CEN_TABLENAME'")
        self.assertEquals(result, expected)

    @patch.object(ITInventory, 'get_rows', autospec=True)
    @patch.object(SourceTable, 'eval', autospec=True)
    def test_get_split_by_mysql_pk(self, mock1, mock2):
        mock2.return_value = [['EVENT_ID']]
        with open(BASE_DIR + '/fixtures/sqls_primary_eval_mock.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()
        mock1.return_value = (0, sqoop_eval_output, '')
        table_obj = ItTable(self.mock_claim_tbl_dict_mysql[0],
                            self.cfg_mgr)
        split_by_obj = Get_Auto_Split(self.cfg_mgr)
        result = split_by_obj.get_split_by_column(table_obj)
        self.assertEquals(result, 'EVENT_ID')

    @patch.object(ITInventory, 'get_rows', autospec=True)
    @patch.object(SourceTable, 'eval', autospec=True)
    def test_get_split_by_mysql_int(self, mock1, mock2):
        mock2.return_value = [['HIT_ID']]
        with open(BASE_DIR + '/fixtures/eval_empty.txt',
                  'r') as file_h:
            sqoop_eval_output1 = file_h.read()
        with open(BASE_DIR + '/fixtures/mysqls_intprimary_eval_mock.txt',
                  'r') as file_h:
            sqoop_eval_output2 = file_h.read()
        mock1.side_effect = [(0, sqoop_eval_output1, ''),
                             (0, sqoop_eval_output2, '')]
        table_obj = ItTable(self.mock_claim_tbl_dict_mysql[0],
                            self.cfg_mgr)
        split_by_obj = Get_Auto_Split(self.cfg_mgr)
        result = split_by_obj.get_split_by_column(table_obj)
        self.assertEquals(result, 'HIT_ID')


class MethodsTest(unittest.TestCase):
    """Test functions."""

    def setUp(self):
        """Setup."""
        self.file_h = open(BASE_DIR +
                           '/fixtures/it_table_gen_split_by.txt', 'r')
        self.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        it_table = {
            'source_database_name': 'fake_database',
            'source_table_name': 'fake_cen_tablename',
            'db_username': 'fake_username',
            'password_file': 'test-passwd',
            'connection_factories': 'test-conn-factory',
            'jdbcurl': 'jdbc:db2://fake.db2:50400/fake_servicename',
            'mappers': '10'
        }
        self.it_table_obj = ItTable(it_table, self.cfg_mgr)

    def tearDown(self):
        """Tear down."""
        pass

    @patch('ibis.utilities.it_table_generation.ITInventory.get_table_mapping',
           autospec=True)
    @patch('ibis.utilities.it_table_generation.ITInventory._connect',
           autospec=True)
    @patch('ibis.utilities.it_table_generation.SourceTable.generate_it_table',
           autospec=True)
    def test_create_auto(self, m_gen_it_table, m_connect, m_g_t_mapping):
        """test auto split by table gen"""
        m_g_t_mapping.return_value = {}
        m_gen_it_table.return_value = ("auto", "test string")
        file_path = create(self.file_h, self.cfg_mgr, 45).name
        self.assertTrue('AUTO_it-tables.txt' in file_path)

    @patch('ibis.utilities.it_table_generation.ITInventory.get_table_mapping',
           autospec=True)
    @patch('ibis.utilities.it_table_generation.ITInventory._connect',
           autospec=True)
    @patch('ibis.utilities.it_table_generation.SourceTable.generate_it_table',
           autospec=True)
    def test_create_manual(self, m_gen_it_table, m_connect, m_g_t_mapping):
        """test manual split by table gen"""
        m_g_t_mapping.return_value = {}
        m_gen_it_table.return_value = ("manual", "test string")
        ret_val = create(self.file_h, self.cfg_mgr, 45)
        self.assertTrue(ret_val is None)

    @patch('ibis.utilities.it_table_generation.SourceTable.get_auto_values',
           autospec=True)
    def test_get_auto_values(self, m_get_auto):
        """test get auto values for split by"""
        m_get_auto.return_value = ('100100', 10, 'spli_by_column')
        ret = get_auto_values(self.it_table_obj, self.cfg_mgr)
        self.assertEquals(ret[1], 10)

    def test_get_src_obj(self):
        """test get src obj"""
        ret = get_src_obj(self.cfg_mgr, self.it_table_obj)
        self.assertTrue(isinstance(ret, DB2Table))


class TeradataTableTest(unittest.TestCase):
    """Test teradataTable methods."""

    def setUp(self):
        """Setup."""
        it_table = {
            'source_database_name': 'fake_database',
            'source_table_name': 'client',
            'db_username': 'fake_username',
            'password_file': 'test-passwd',
            'connection_factories': 'test-conn-factory',
            'jdbcurl': 'jdbc:teradata:',
            'mappers': '10'
        }
        self.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        it_table_obj = ItTable(it_table, self.cfg_mgr)
        self.td_obj = TeradataTable(self.cfg_mgr, it_table_obj)
        self.mock_claim_tbl_dict = [{
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
        self.mock_warehouse_tbl_dict = [{
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

    @patch.object(ITInventory, 'get_rows', autospec=True)
    @patch.object(SourceTable, 'eval', autospec=True)
    def test_get_split_by_teradata_pk(self, mock1, mock2):
        mock2.return_value = [['KEY']]
        with open(BASE_DIR + '/fixtures/td_primary_eval_mock.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()
        mock1.return_value = (0, sqoop_eval_output, '')
        table_obj = ItTable(self.mock_claim_tbl_dict[0], self.cfg_mgr)
        split_by_obj = Get_Auto_Split(self.cfg_mgr)
        result = split_by_obj.get_split_by_column(table_obj)
        self.assertEquals(result, "KEY")

    @patch.object(ITInventory, 'get_rows', autospec=True)
    @patch.object(SourceTable, 'eval', autospec=True)
    def test_get_split_by_teradata_int(self, mock1, mock2):
        mock2.return_value = [['LOAD_CTL_KEY']]
        with open(BASE_DIR + '/fixtures/td_valprimary_eval_mock.txt',
                  'r') as file_h:
            sqoop_eval_output1 = file_h.read()
        with open(BASE_DIR + '/fixtures/td_noprimary_eval_mock.txt',
                  'r') as file_h:
            sqoop_eval_output2 = file_h.read()
        mock1.side_effect = [(0, sqoop_eval_output1, ''),
                             (0, sqoop_eval_output2, '')]
        table_obj = ItTable(self.mock_claim_tbl_dict[0], self.cfg_mgr)
        split_by_obj = Get_Auto_Split(self.cfg_mgr)
        result = split_by_obj.get_split_by_column(table_obj)
        self.assertEquals(result, "LOAD_CTL_KEY")

    @patch.object(ITInventory, 'get_rows', autospec=True)
    def test_get_split_by_teradata_warehouse(self, mock1):
        mock1.return_value = [['LOAD_CTL_KEY']]
        table_obj = ItTable(self.mock_warehouse_tbl_dict[0], self.cfg_mgr)
        split_by_obj = Get_Auto_Split(self.cfg_mgr)
        result = split_by_obj.get_split_by_column(table_obj)
        self.assertEquals(result, "LOAD_CTL_KEY")

    def tearDown(self):
        """Tear down."""
        pass


class SqlserverTableTest(unittest.TestCase):
    """Test teradataTable methods."""

    def setUp(self):
        """Setup."""
        it_table = {
            'source_database_name': 'fake_database',
            'source_table_name': 'client',
            'db_username': 'fake_username',
            'password_file': 'test-passwd',
            'connection_factories': 'test-conn-factory',
            'jdbcurl': 'jdbc:sqlserver:',
            'mappers': '10'
        }
        self.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        it_table_obj = ItTable(it_table, self.cfg_mgr)
        self.sql_obj = SqlServerTable(self.cfg_mgr, it_table_obj)
        self.mock_claim_tbl_dict_sqlserver = [{
            'full_table_name': 'claim.fake_database_fake_clm_tablename',
            'domain': 'claim',
            'target_dir': 'mdm/claim/fake_database/fake_clm_tablename',
            'split_by': 'fake_split_by', 'mappers': '20',
            'jdbcurl': 'jdbc:sqlserver://fake.teradata/'
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

    @patch.object(ITInventory, 'get_rows', autospec=True)
    @patch.object(SourceTable, 'eval', autospec=True)
    def test_get_split_by_sqlserver_pk(self, mock1, mock2):
        mock2.return_value = [['EVENT_ID']]
        with open(BASE_DIR + '/fixtures/sqls_primary_eval_mock.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()
        mock1.return_value = (0, sqoop_eval_output, '')
        table_obj = ItTable(self.mock_claim_tbl_dict_sqlserver[0],
                            self.cfg_mgr)
        split_by_obj = Get_Auto_Split(self.cfg_mgr)
        result = split_by_obj.get_split_by_column(table_obj)
        self.assertEquals(result, 'EVENT_ID')

    @patch.object(ITInventory, 'get_rows', autospec=True)
    @patch.object(SourceTable, 'eval', autospec=True)
    def test_get_split_by_sqlserver_int(self, mock1, mock2):
        mock2.return_value = [['HIT_ID']]
        with open(BASE_DIR + '/fixtures/eval_empty.txt',
                  'r') as file_h:
            sqoop_eval_output1 = file_h.read()
        with open(BASE_DIR + '/fixtures/sqls_intprimary_eval_mock.txt',
                  'r') as file_h:
            sqoop_eval_output2 = file_h.read()
        mock1.side_effect = [(0, sqoop_eval_output1, ''),
                             (0, sqoop_eval_output2, '')]
        table_obj = ItTable(self.mock_claim_tbl_dict_sqlserver[0],
                            self.cfg_mgr)
        split_by_obj = Get_Auto_Split(self.cfg_mgr)
        result = split_by_obj.get_split_by_column(table_obj)
        self.assertEquals(result, 'HIT_ID')

    def tearDown(self):
        """Tear down."""
        pass

if __name__ == "__main__":
    unittest.main()
else:
    loader = unittest.TestLoader()

    test_classes_to_run = [SourceTableTest, OracleTableTest,
                           DB2TableTest, MethodsTest, MySQLTableTest,
                           SqlserverTableTest, TeradataTableTest]
    # Create test suite
    it_table_gen_test_suite = []
    for test_class in test_classes_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        it_table_gen_test_suite.append(suite)
