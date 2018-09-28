"""Test quality assurance export code.

Usage: run nose2 in the current directory
"""
import unittest
import os
from mock import patch, MagicMock
from lib.ingest.sqoop_utils import SqoopUtils
from lib.ingest.quality_assurance_export import SourceTable, LogHandler, \
    OracleTable, TeraDataTable, MSSqlTable, TargetTable, TableValidation, \
    ChecksBalancesExportManager
from lib import ingest
from lib.ingest.py_hdfs import PyHDFS
from lib.ingest.checks_and_balances_export import Workflow

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class LogHandlerTest(unittest.TestCase):
    """Test LogHandler class."""

    def test_init(self):
        """test."""
        log_object = LogHandler('qa', 'host')
        self.assertEqual('qa', log_object.source_table)
        self.assertEqual('host', log_object.host_name)


class SourceTableTest(unittest.TestCase):
    """Test SourceTable class."""

    def setUp(self):
        self.table = 'qa.fake_database_fake_cen_tablename'
        self.source_database_name = 'qa'
        self.source_table_name = 'fake_database_fake_cen_tablename'
        self.srcObj = SourceTable(self.table, 'host')

    def tearDown(self):
        """test."""
        # self.popen_patcher.stop()
        pass

    def test_init(self):
        """test."""
        self.assertEqual(self.srcObj.table, self.table)
        self.assertEqual(self.srcObj.source_database_name,
                         self.source_database_name)
        self.assertEqual(self.srcObj.source_table_name, self.source_table_name)

    @patch('lib.ingest.quality_assurance_export.ImpalaConnect', autospec=True)
    def test_get_source_row_count(self, mock_source_table):
        """test."""
        mock_source_table.run_query.return_value = [['10000']]
        self.assertEqual(self.srcObj.get_source_row_count(), 10000)


class TargetTableTest(unittest.TestCase):
    """Test TargetTable class."""

    def setUp(self):
        self.table = 'TEST_DB.test'
        self.database = 'TEST_DB'
        self.target_table = 'test'
        self.jars = 'test_jars'
        self.jceks = 'test_jceks'
        self.jdbc_url = 'jdbc:sqlserver://fake.test.sqlserver'
        self.connection_factories = 'test_conn_fac'
        self.user_name = 'test_username'
        self.password_alias = 'test_passwd'
        self.target_schema = 'dbo'
        self.ttObj = TargetTable(self, self.jars, self.jdbc_url,
                                 self.connection_factories,
                                 self.user_name, self.jceks,
                                 self.password_alias, self.target_schema)

    def tearDown(self):
        """test."""
        # self.popen_patcher.stop()
        pass


class OracleTableTest(unittest.TestCase):
    """test."""

    def setUp(self):
        """Test setup."""
        params = {
            'domain': 'test_domain',
            'database': 'TEST_DB',
            'target_table': 'test',
            'target_dir': 'test_tar_dir',
            'jars': 'test_jars',
            'jdbc_url': 'test_jdbc',
            'connection_factories': 'test_conn_fac',
            'user_name': 'test_username',
            'jceks': 'test_jceks',
            'password_alias': 'test_passwd',
            'target_host': 'test_t_host',
            'target_schema': 'None'
        }
        self.target_table = params['database'] + '.' + params['target_table']
        self.ora_obj = OracleTable(
            params['jars'], params['jdbc_url'], params['connection_factories'],
            params['user_name'], params['jceks'], params['password_alias'],
            self.target_table, params['target_schema'])
        self.row_count_expected = \
            ("\n------------------------\n| COUNT(*)             |"
             "\n------------------------\n| 10000                |"
             "\n------------------------\n")

    def tearDown(self):
        """test."""
        pass

    @patch.object(TargetTable, 'eval', autospec=True)
    def test_get_row_count(self, mock_eval):
        """test."""
        mock_eval.return_value = (0, self.row_count_expected, "")
        test_row_count = self.ora_obj.get_target_row_count()
        self.assertEqual(test_row_count, 10000)

    def test_build_row_count_query(self):
        """test."""
        expected = "SELECT COUNT(*) FROM TEST_DB.test"
        test_output = self.ora_obj.build_row_count_query()
        self.assertEqual(expected, test_output)

    def test_clean_query_result(self):
        """test."""
        expected = ['| COUNT(*)             |', '| 10000                |']
        test_output = self.ora_obj._clean_query_result(self.row_count_expected)
        self.assertEqual(expected, test_output)


class DB2TableTest(unittest.TestCase):
    """test."""

    def setUp(self):
        """Test setup."""
        params = {
            'domain': 'test_domain',
            'database': 'test_db',
            'target_table': 'test_table_name',
            'target_dir': 'test_tar_dir',
            'jars': 'test_jars',
            'jdbc_url': 'test_jdbc',
            'connection_factories': 'test_conn_fac',
            'user_name': 'test_username',
            'jceks': 'test_jceks',
            'password_alias': 'test_passwd',
            'target_host': 'test_t_host',
            'target_schema': 'None'
        }
        self.source_table = params['database'] + '.' + params['target_table']
        self.td_obj = TeraDataTable(
            params['jars'], params['jdbc_url'], params['connection_factories'],
            params['user_name'], params['jceks'], params['password_alias'],
            self.source_table, params['target_schema'])

    def tearDown(self):
        """test."""
        pass


class TeraDataTableTest(unittest.TestCase):
    """test."""

    def setUp(self):
        """Test setup."""
        params = {
            'domain': 'test_domain',
            'database': 'test_db',
            'target_table': 'test_table_name',
            'target_dir': 'test_tar_dir',
            'jars': 'test_jars',
            'jdbc_url': 'test_jdbc',
            'connection_factories': 'test_conn_fac',
            'user_name': 'test_username',
            'jceks': 'test_jceks',
            'password_alias': 'test_passwd',
            'target_host': 'test_t_host',
            'target_schema': 'None'
        }
        self.source_table = params['database'] + '.' + params['target_table']
        self.td_obj = TeraDataTable(
            params['jars'], params['jdbc_url'], params['connection_factories'],
            params['user_name'], params['jceks'], params['password_alias'],
            self.source_table, params['target_schema'])

    def tearDown(self):
        """test."""
        pass


class MSSqlTableTest(unittest.TestCase):
    """test."""

    def setUp(self):
        """Test setup."""
        params = {
            'domain': 'test_domain',
            'database': 'test_db',
            'target_table': 'test_table_name',
            'target_dir': 'test_tar_dir',
            'jars': 'test_jars',
            'jdbc_url': 'jdbc:sqlserver://fake.test.sqlserver'
                        ':1433;database=test_db',
            'connection_factories': 'test_conn_fac',
            'user_name': 'test_username',
            'jceks': 'test_jceks',
            'password_alias': 'test_passwd',
            'target_host': 'test_t_host',
            'target_schema': 'dbo'
        }
        self.source_table = params['database'] + '.' + params['target_table']
        self.sql_obj = MSSqlTable(
            params['jars'], params['jdbc_url'], params['connection_factories'],
            params['user_name'], params['jceks'], params['password_alias'],
            self.source_table, params['target_schema'])

    def tearDown(self):
        """test."""
        pass


class TableValidationTest(unittest.TestCase):
    """test."""

    @patch('lib.ingest.quality_assurance_export.logger')
    def setUp(self, m_logger):
        """Test setup."""
        self.params = {
            'database': 'fake_database',
            'target_table': 'fake_mem_tablename',
            'jars': 'test-jars',
            'jdbc_url': 'jdbc:oracle:thin:@//fake.oracle:'
                        '1521/fake_servicename',
            'connection_factories': 'test_cf',
            'user_name': 'fake_username',
            'jceks': 'jceks',
            'password_alias': 'passwd-alias',
            'host_name': 'fake.dev.edgenode',
            'domain': 'member',
            'target_schema': 'None',
            'oozie_url': 'None',
            'qa_exp_results_dir': '/user/hive/warehouse/ibis.db/qa_exp_res'
        }
        self.table_val_obj = TableValidation(
            'text stage.db_table', 'domain.db_table', self.params)

    def tearDown(self):
        """test."""
        pass

    @patch('lib.ingest.quality_assurance_export.logger')
    def test_count_matches(self, m_logger):
        """test count matches"""
        bool_test = self.table_val_obj.count_matches(0, 0)
        self.assertTrue(bool_test)

        bool_test = self.table_val_obj.count_matches(100, 96)
        self.assertTrue(bool_test)

        bool_test = self.table_val_obj.count_matches(100, 92)
        self.assertFalse(bool_test)

        bool_test = self.table_val_obj.count_matches(100, 106)
        self.assertFalse(bool_test)

        bool_test = self.table_val_obj.count_matches(100, 103)
        self.assertFalse(bool_test)

        bool_test = self.table_val_obj.count_matches(100, 98)
        self.assertTrue(bool_test)

    @patch('lib.ingest.quality_assurance_export.ImpalaConnect', autospec=True)
    @patch.object(SqoopUtils, 'eval', autospec=True)
    @patch('lib.ingest.quality_assurance_export.logger')
    @patch.object(TargetTable, 'get_target_row_count', return_value='2')
    @patch.object(ChecksBalancesExportManager, 'validate_in_and_out_counts',
                  return_value='2')
    def test_start_data_sampling(self, m_logger, m_eval, m3, m4, m5):
        """test data sampling qa for sqlserver"""
        self.params['jdbc_url'] = ('jdbc:sqlserver://fake.sqlserver:1433;database=FAKE_DATABASE')
        table_val_obj = TableValidation(
            'parquet_stage.db_table', 'domain.db_table', self.params)
        action = 'new_action'
        self.assertFalse(table_val_obj.start_data_sampling(action))

    @patch.object(TableValidation, "start_data_sampling", return_value=False)
    @patch.object(ChecksBalancesExportManager, "check_if_workflow_actions")
    @patch.object(PyHDFS, "insert_update")
    @patch('os.environ', return_value='IMPALA_HOST')
    def test_insert_hive(self, m_impala_host, m_pyhdfs, m_get, m_sam):
        args = """test source_database_name source_table_name database
        target_table jars jdbc_url connection_factories user_name
         jceks password_alias host_name domain target_schema
          oozie_url workflow_name false qa_exp_results_dir""".split()

        with open(os.path.join(BASE_DIR,
                               'fixtures/test_job_action_export.json'),
                  'r') as \
                content_file:
            one_action_json = content_file.read()
        m_get.return_value = Workflow(one_action_json).get_actions()

        def getitem(index):
            return args[index]
        mock_sys = MagicMock()
        mock_sys.__getitem__.side_effect = getitem
        with patch('sys.argv', mock_sys):
            with self.assertRaises(SystemExit):
                ingest.quality_assurance_export.main()

if __name__ == '__main__':
    unittest.main()
else:
    loader = unittest.TestLoader()

    test_classes_to_run = [SourceTableTest, TargetTableTest,
                           OracleTableTest, TeraDataTableTest, DB2TableTest,
                           MSSqlTableTest, TableValidationTest]
    # Create test suite
    qa_test_suite_export = []
    for test_class in test_classes_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        qa_test_suite_export.append(suite)
