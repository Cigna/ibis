"""Test quality assurance code.

Usage: run nose2 in the current directory
"""
import unittest
import os
from mock import patch, MagicMock
from lib.ingest.quality_assurance import ColumnDDL, SourceTable, \
    OracleTable, TeraDataTable, MSSqlTable, PostgreSql, \
    TargetTable, TableValidation
from lib.ingest.tests.fixtures.qa_impala_ddl import DDL_TD
from lib.ingest.tests.fixtures.qa_data_sample_sqlserver import \
    DATA_SAMPLE_HIVE_DDL, DATA_SAMPLE_HIVE_ROWS
from lib.ingest.oozie_ws_helper import Workflow, ChecksBalancesManager
from lib.ingest import quality_assurance
from lib.ingest.py_hdfs import PyHDFS


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class ColumnDDLTest(unittest.TestCase):
    """Test ColumnDDL class."""

    def test_class_instance(self):
        """test."""
        col_obj = ColumnDDL('VENDR_NM', 'VARCHAR', '10')
        self.assertEqual('vendr_nm', col_obj.column_name)
        self.assertEqual('varchar', col_obj.data_type)
        self.assertEqual(10, col_obj.data_len)

    def test_data_len(self):
        """Test data len."""
        col_obj = ColumnDDL('VENDR_NM', 'VARCHAR', None)
        self.assertEqual(None, col_obj.data_len)
        self.assertEqual(None, col_obj.precision)
        self.assertEqual(None, col_obj.scale)
        col_obj = ColumnDDL('VENDR_NM', 'NUMBER', '10,5')
        self.assertEqual(10, col_obj.data_len)
        self.assertEqual(10, col_obj.precision)
        self.assertEqual(5, col_obj.scale)
        col_obj = ColumnDDL('VENDR_NM', 'CHAR', '10')
        self.assertEqual(10, col_obj.data_len)
        self.assertEqual(None, col_obj.precision)
        self.assertEqual(None, col_obj.scale)


class TargetTableTest(unittest.TestCase):
    """Test TargetTable class."""

    def setUp(self):
        """Test ColumnDDL class."""
        self.table = 'qa.fake_database_fake_cen_tablename'
        self.database_name = 'qa'
        self.table_name = 'fake_database_fake_cen_tablename'
        self.ttObj = TargetTable(self.table, 'host')

    def tearDown(self):
        """test."""
        # self.popen_patcher.stop()
        pass

    def test_init(self):
        """test."""
        self.assertEqual(self.ttObj.table, self.table)
        self.assertEqual(self.ttObj.database_name, self.database_name)
        self.assertEqual(self.ttObj.table_name, self.table_name)

    @patch('lib.ingest.quality_assurance.ImpalaConnect', autospec=True)
    def test_get_row_count(self, mock_target_table):
        """test."""
        mock_target_table.run_query.return_value = [['10000']]
        self.assertEqual(self.ttObj.get_row_count(), 10000)

    def test_build_ddl_query(self):
        """test."""
        expected = "describe qa.fake_database_fake_cen_tablename;"
        self.assertEqual(self.ttObj.build_ddl_query(), expected)

    @patch('lib.ingest.quality_assurance.ImpalaConnect', autospec=True)
    def test_get_ddl(self, mock_impala_conn):
        """test fetch hive ddl"""
        mock_impala_conn.run_query.return_value = [
            ('fnty_cd', 'varchar(5)', ''), ('fnty_eff_dt', 'timestamp', ''),
            ('fnty_desc', 'varchar(45)', ''),
            ('fnty_term_dt', 'timestamp', ''),
            ('fnty_update_dt', 'timestamp', ''),
            ('fnty_update_usr_id', 'varchar(15)', ''),
            ('onef_create_dt', 'timestamp', ''),
            ('ingest_timestamp', 'string', ''),
            ('incr_ingest_timestamp', 'string', '')]
        ddl_list = self.ttObj.get_ddl()
        self.assertEqual(len(ddl_list), 7)
        self.assertEqual(ddl_list[0].column_name, 'fnty_cd')
        self.assertEqual(ddl_list[0].data_type, 'varchar')
        self.assertEqual(ddl_list[0].data_len, 5)
        self.assertEqual(ddl_list[0].precision, None)
        self.assertEqual(ddl_list[0].scale, None)


class OracleTableTest(unittest.TestCase):
    """test."""

    def setUp(self):
        """Test setup."""
        params = {
            'domain': 'test_domain',
            'database': 'test_db',
            'table_name': 'test_table_name',
            'target_dir': 'test_tar_dir',
            'jars': 'test_jars',
            'jdbc_url': 'test_jdbc',
            'connection_factories': 'test_conn_fac',
            'user_name': 'test_username',
            'jceks': 'test_jceks',
            'password_alias': 'test_passwd',
            'target_host': 'test_t_host',
            'schema': 'None'
        }
        self.source_table = params['database'] + '.' + params['table_name']
        self.ora_obj = OracleTable(
            params['jars'], params['jdbc_url'], params['connection_factories'],
            params['user_name'], params['jceks'], params['password_alias'],
            self.source_table, params['schema'])
        self.row_count_expected = \
            ("\n------------------------\n| COUNT(*)             |"
             "\n------------------------\n| 10000                |"
             "\n------------------------\n")

    def tearDown(self):
        """test."""
        pass

    @patch.object(SourceTable, 'eval', autospec=True)
    def test_get_row_count(self, mock_eval):
        """test."""
        mock_eval.return_value = (0, self.row_count_expected, "")
        test_row_count = self.ora_obj.get_row_count()
        self.assertEqual(test_row_count, 10000)

    def test_build_row_count_query(self):
        """test."""
        expected = "SELECT COUNT(*) FROM TEST_DB.TEST_TABLE_NAME"
        test_output = self.ora_obj.build_row_count_query()
        self.assertEqual(expected, test_output)

    def test_clean_query_result(self):
        """test."""
        expected = ['| COUNT(*)             |', '| 10000                |']
        test_output = self.ora_obj._clean_query_result(self.row_count_expected)
        self.assertEqual(expected, test_output)

    def test_fetch_rows_sqoop(self):
        """Test rows cleanup."""
        with open(BASE_DIR + '/fixtures/ddl_ora.txt', 'r') as file_handler:
            sqoop_output = file_handler.read()

        column_names, rows = self.ora_obj.fetch_rows_sqoop(sqoop_output)
        self.assertEqual(len(column_names), 7)
        self.assertEqual(len(rows), 12)
        self.assertEqual(rows[0][1], 'VARCHAR2')
        self.assertEqual(rows[0][2], '2')
        column_names, rows = self.ora_obj.fetch_rows_sqoop(
            sqoop_output, strip_col_val=False)
        self.assertEqual(len(column_names), 7)
        self.assertEqual(len(rows), 12)
        self.assertEqual(rows[0][1], 'VARCHAR2            ')
        self.assertEqual(rows[0][2], '2                   ')

    @patch.object(SourceTable, 'eval', autospec=True)
    def test_get_ddl(self, m_eval):
        """test get ddl oracle"""
        with open(BASE_DIR + '/fixtures/ddl_ora.txt', 'r') as file_handler:
            sqoop_ddl = file_handler.read()

        with open(BASE_DIR + '/fixtures/max_len_ora.txt', 'r') as file_handler:
            sqoop_max_len = file_handler.read()

        m_eval.side_effect = [(0, sqoop_ddl, ''), (0, sqoop_max_len, '')]
        ddls = self.ora_obj.get_ddl()
        for ddl in ddls:
            if ddl.column_name == 'fake_col_5':
                number_test = ddl
            if ddl.column_name == 'fake_col_7':
                timestamp_test = ddl
            if ddl.column_name == 'fake_col_11':
                date_test = ddl
            if ddl.column_name == 'fake_col_3':
                varchar2_test = ddl

        self.assertEqual(number_test.data_type, 'number')
        self.assertEqual(number_test.data_len, 38)
        self.assertEqual(number_test.precision, 38)
        self.assertEqual(number_test.scale, 0)

        self.assertEqual(timestamp_test.data_type, 'timestamp')
        self.assertEqual(timestamp_test.data_len, 6)
        self.assertEqual(timestamp_test.precision, None)
        self.assertEqual(timestamp_test.scale, None)

        self.assertEqual(date_test.data_type, 'date')
        self.assertEqual(date_test.data_len, 7)
        self.assertEqual(date_test.precision, None)
        self.assertEqual(date_test.scale, None)

        self.assertEqual(varchar2_test.data_type, 'varchar2')
        self.assertEqual(varchar2_test.data_len, 9)
        self.assertEqual(varchar2_test.precision, None)
        self.assertEqual(varchar2_test.scale, None)


class TeraDataTableTest(unittest.TestCase):
    """test."""

    def setUp(self):
        """Test setup."""
        params = {
            'domain': 'test_domain',
            'database': 'test_db',
            'table_name': 'test_table_name',
            'target_dir': 'test_tar_dir',
            'jars': 'test_jars',
            'jdbc_url': 'test_jdbc',
            'connection_factories': 'test_conn_fac',
            'user_name': 'test_username',
            'jceks': 'test_jceks',
            'password_alias': 'test_passwd',
            'target_host': 'test_t_host',
            'schema': 'None'
        }
        self.source_table = params['database'] + '.' + params['table_name']
        self.td_obj = TeraDataTable(
            params['jars'], params['jdbc_url'], params['connection_factories'],
            params['user_name'], params['jceks'], params['password_alias'],
            self.source_table, params['schema'])

    def tearDown(self):
        """test."""
        pass

    @patch.object(SourceTable, 'eval', autospec=True)
    def test_get_ddl(self, m_eval):
        """test teradata ddl sqoop"""
        with open(BASE_DIR + '/fixtures/ddl_td.txt', 'r') as file_handler:
            sqoop_ddl = file_handler.read()

        m_eval.side_effect = [(0, sqoop_ddl, '')]
        ddls = self.td_obj.get_ddl()
        self.assertEqual(len(ddls), 7)


class MSSqlTableTest(unittest.TestCase):
    """test."""

    def setUp(self):
        """Test setup."""
        params = {
            'domain': 'test_domain',
            'database': 'test_db',
            'table_name': 'test_table_name',
            'target_dir': 'test_tar_dir',
            'jars': 'test_jars',
            'jdbc_url': 'jdbc:sqlserver://fake.test.sqlserver'
                        ':1433;database=test_db',
            'connection_factories': 'test_conn_fac',
            'user_name': 'test_username',
            'jceks': 'test_jceks',
            'password_alias': 'test_passwd',
            'target_host': 'test_t_host',
            'schema': 'dbo'
        }
        self.source_table = params['database'] + '.' + params['table_name']
        self.sql_obj = MSSqlTable(
            params['jars'], params['jdbc_url'], params['connection_factories'],
            params['user_name'], params['jceks'], params['password_alias'],
            self.source_table, params['schema'])

    def tearDown(self):
        """test."""
        pass

    @patch.object(SourceTable, 'eval', autospec=True)
    def test_get_ddl(self, m_eval):
        """test teradata ddl sqoop"""
        with open(BASE_DIR + '/fixtures/ddl_sql_server.txt', 'r') \
                as file_handler:
            sqoop_ddl = file_handler.read()

        m_eval.side_effect = [(0, sqoop_ddl, '')]
        ddls = self.sql_obj.get_ddl()
        self.assertEqual(len(ddls), 20)


class PostgreSqlTest(unittest.TestCase):
    """test."""

    def setUp(self):
        """Test setup."""
        params = {
            'domain': 'test_domain',
            'database': 'test_db',
            'table_name': 'test_table_name',
            'target_dir': 'test_tar_dir',
            'jars': 'test_jars',
            'jdbc_url': 'jdbc:postgresql://fake.test.sqlserver'
                        ':1433;database=test_db',
            'connection_factories': 'test_conn_fac',
            'user_name': 'test_username',
            'jceks': 'test_jceks',
            'password_alias': 'test_passwd',
            'target_host': 'test_t_host',
            'schema': 'dbo'
        }
        self.source_table = params['database'] + '.' + params['table_name']
        self.sql_obj = PostgreSql(
            params['jars'], params['jdbc_url'], params['connection_factories'],
            params['user_name'], params['jceks'], params['password_alias'],
            self.source_table, params['schema'])

    def tearDown(self):
        """test."""
        pass

    @patch.object(SourceTable, 'eval', autospec=True)
    def test_get_ddl(self, m_eval):
        """test postgresql ddl sqoop"""
        with open(BASE_DIR + '/fixtures/ddl_postgresql.txt', 'r') \
                as file_handler:
            sqoop_ddl = file_handler.read()

        m_eval.side_effect = [(0, sqoop_ddl, '')]
        ddls = self.sql_obj.get_ddl()
        self.assertEqual(len(ddls), 39)

    @patch.object(SourceTable, 'eval', autospec=True)
    def test_get_ddl_val(self, m_eval):
        """test postgresql ddl sqoop"""
        with open(BASE_DIR + '/fixtures/ddl_postgresql.txt', 'r') \
                as file_handler:
            sqoop_ddl = file_handler.read()

        m_eval.side_effect = [(0, sqoop_ddl, '')]
        ddls = self.sql_obj.get_ddl()
        val = ddls[0].data_type
        self.assertEqual(val, "datetime")
        valp = ddls[37].precision
        vals = ddls[37].scale
        self.assertEqual(valp, 53)
        self.assertEqual(vals, 4)


class TableValidationTest(unittest.TestCase):
    """test."""

    @patch('lib.ingest.quality_assurance.logger')
    def setUp(self, m_logger):
        """Test setup."""
        self.params = {
            'database': 'fake_database',
            'table_name': 'fake_mem_tablename',
            'jars': 'test-jars',
            'jdbc_url': 'jdbc:oracle:thin:@//fake.oracle:'
                        '1521/fake_servicename',
            'connection_factories': 'test_cf',
            'user_name': 'fake_username',
            'password_file': 'jceks#passwd-alias',
            'impala_host': 'fake.dev.edgenode',
            'hive_host': 'fake.dev.edgenode',
            'domain': 'member',
            'ingestion_type': 'full_ingest',
            'schema': 'None',
            'oozie_url': 'http://fake.dev.oozie:25007/oozie/v2/',
            'workflow_name': 'test_workflow',
            'qa_results_tbl_path': '/user/hive/warehouse/ibis.db/qa_resultsv2'
        }
        self.table_val_obj = TableValidation(
            'parquet_stage.db_table', 'domain.db_table', self.params)

    def tearDown(self):
        """test."""
        pass

    @patch('lib.ingest.quality_assurance.logger')
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

    @patch('lib.ingest.quality_assurance.ImpalaConnect', autospec=True)
    @patch.object(SourceTable, 'eval', autospec=True)
    @patch('lib.ingest.quality_assurance.logger')
    def test_start_full_ingest_qa_td(self, m_logger, m_eval, mock_impala_conn):
        """test full ingest qa for Teradata"""
        with open(BASE_DIR + '/fixtures/qa_ddl_td.txt', 'r') \
                as file_handler:
            sqoop_ddl = file_handler.read()
        m_eval.side_effect = [(0, sqoop_ddl, '')]
        mock_impala_conn.run_query.return_value = DDL_TD
        self.params['jdbc_url'] = ('jdbc:teradata://fake.teradata/'
                                   'database=fake_database')
        table_val_obj = TableValidation(
            'parquet_stage.db_table', 'domain.db_table', self.params)
        self.assertTrue(table_val_obj.start_full_ingest_qa())

    @patch('lib.ingest.quality_assurance.ImpalaConnect', autospec=True)
    @patch.object(SourceTable, 'eval', autospec=True)
    @patch('lib.ingest.quality_assurance.logger')
    def test_start_data_sampling(self, m_logger, m_eval, mock_impala_conn):
        """test data sampling qa for sqlserver"""
        _path = BASE_DIR + '/fixtures/qa_data_sample_ddl_sqlserver.txt'
        with open(_path, 'r') as file_handler:
            sqoop_ddl = file_handler.read()
        _path = BASE_DIR + '/fixtures/qa_data_sample_rows_sqlserver.txt'
        with open(_path, 'r') as file_handler:
            sqoop_rows = file_handler.read()
        m_eval.side_effect = [(0, sqoop_ddl, ''), (0, sqoop_rows, '')]
        mock_impala_conn.run_query.side_effect = [DATA_SAMPLE_HIVE_DDL,
                                                  DATA_SAMPLE_HIVE_ROWS[0],
                                                  DATA_SAMPLE_HIVE_ROWS[1],
                                                  DATA_SAMPLE_HIVE_ROWS[2]]
        self.params['jdbc_url'] = ('jdbc:sqlserver://fake.sqlserver:1433;database=FAKE_DATABASE')
        table_val_obj = TableValidation(
            'parquet_stage.db_table', 'domain.db_table', self.params)
        self.assertTrue(table_val_obj.start_data_sampling())

    @patch.object(PyHDFS, "insert_update")
    @patch.object(TableValidation, 'start_incremental', return_value=False)
    @patch.object(ChecksBalancesManager, "check_if_workflow")
    @patch('os.environ', return_value='IMPALA_HOST')
    def test_check_if_workflow(self, m_impala_host, m_get, m_incr, m_pyhdfs):
        with open(os.path.join(BASE_DIR,
                               'fixtures/test_job_action.json'), 'r') as \
                content_file:
            one_action_json = content_file.read()
        m_get.return_value = Workflow(one_action_json)
        args = """commandname database table_name jars jdbc_url
         connection_factories user_name password_file domain
         incremental schema oozie_url workflow_name
         impala_host qa_results_tbl_path""".split()

        def getitem(index):
            return args[index]
        mock_sys = MagicMock()
        mock_sys.__getitem__.side_effect = getitem
        with patch('sys.argv', mock_sys):
            with self.assertRaises(SystemExit):
                quality_assurance.main()

    @patch.object(PyHDFS, "insert_update")
    @patch.object(TableValidation, 'start_full_ingest_qa', return_value=False)
    @patch('os.environ', return_value='IMPALA_HOST')
    def test_full_ingest(self, m_impala_host, m_incr, m_pyhdfs):
        args = """commandname database table_name jars jdbc_url
         connection_factories user_name password_file domain
         full_ingest schema oozie_url workflow_name
         impala_host qa_results_tbl_path""".split()

        def getitem(index):
            return args[index]
        mock_sys = MagicMock()
        mock_sys.__getitem__.side_effect = getitem
        with patch('sys.argv', mock_sys):
            with self.assertRaises(SystemExit):
                quality_assurance.main()

    @patch.object(PyHDFS, "insert_update")
    @patch.object(TableValidation, 'start_data_sampling', return_value=False)
    @patch('os.environ', return_value='IMPALA_HOST')
    def test_main_start_data_sampling(self, m_impala_host, m_incr, m_pyhdfs):
        args = """commandname database table_name jars jdbc_url
         connection_factories user_name password_file domain
         standalone_qa_sampling schema oozie_url workflow_name
         impala_host qa_results_tbl_path""".split()

        def getitem(index):
            return args[index]
        mock_sys = MagicMock()
        mock_sys.__getitem__.side_effect = getitem
        with patch('sys.argv', mock_sys):
            with self.assertRaises(SystemExit):
                quality_assurance.main()

    @patch.object(ChecksBalancesManager, 'validate_in_and_out_counts')
    @patch.object(TargetTable, 'get_row_count')
    @patch('lib.ingest.quality_assurance.ImpalaConnect', autospec=True)
    @patch.object(PyHDFS, "insert_update")
    @patch.object(ChecksBalancesManager, "check_if_workflow")
    @patch.object(SourceTable, 'eval', autospec=True)
    @patch('os.environ', return_value='IMPALA_HOST')
    def test_start_incremental(self, m_impala_host, m_eval, m_get, m_pyhdfs,
                               mock_impala_conn, target_count, source_count):
        mock_impala_conn.run_query.return_value = [
            ('fnty_cd', 'varchar(5)', ''), ('fnty_eff_dt', 'timestamp', ''),
            ('fnty_desc', 'varchar(45)', ''),
            ('fnty_term_dt', 'timestamp', ''),
            ('fnty_update_dt', 'timestamp', ''),
            ('fnty_update_usr_id', 'varchar(15)', ''),
            ('onef_create_dt', 'timestamp', ''),
            ('ingest_timestamp', 'string', ''),
            ('incr_ingest_timestamp', 'string', '')]

        with open(BASE_DIR + '/fixtures/ddl_ora.txt', 'r') as file_handler:
            sqoop_ddl = file_handler.read()
        m_eval.return_value = (0, sqoop_ddl, '')

        with open(os.path.join(BASE_DIR,
                               'fixtures/test_job_action.json'), 'r') as \
                content_file:
            one_action_json = content_file.read()
        m_get.return_value = Workflow(one_action_json)

        args = """commandname database table_name jars jdbc_url
         connection_factories user_name password_file domain
         incremental schema oozie_url workflow_name
         impala_host qa_results_tbl_path""".split()

        target_count.return_value = 100
        source_count.return_value = 100

        def getitem(index):
            return args[index]
        mock_sys = MagicMock()
        mock_sys.__getitem__.side_effect = getitem
        with patch('sys.argv', mock_sys):
            with self.assertRaises(SystemExit):
                quality_assurance.main()


if __name__ == '__main__':
    unittest.main()
else:
    loader = unittest.TestLoader()

    test_classes_to_run = [ColumnDDLTest, TargetTableTest, OracleTableTest,
                           TeraDataTableTest, MSSqlTableTest, PostgreSqlTest,
                           TableValidationTest]
    # Create test suite
    qa_test_suite = []
    for test_class in test_classes_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        qa_test_suite.append(suite)
