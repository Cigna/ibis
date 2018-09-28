"""Test quality assurance export code.

Usage: run nose2 in the current directory
"""
import unittest
import os
from mock import patch, MagicMock
from lib.ingest.sqoop_utils import SqoopUtils
from lib.ingest.pre_quality_assurance_export import SourceTable, ColumnDDL, \
    OracleTable, DB2Table, TeraDataTable, MSSqlTable, TargetTable, \
    TableValidation, LogHandler, MySqlTable, main
from lib.ingest.tests.fixtures.qa_data_sample_sqlserver import \
    DATA_SAMPLE_HIVE_DDL_EXPORT, SAMPLE_HIVE_EXPORT_FALSE, \
    DATA_SAMPLE_HIVE_DDL_TERA
from lib.ingest.py_hdfs import PyHDFS

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class LogHandlerTest(unittest.TestCase):
    """Test LogHandler class."""

    def test_init(self):
        """test."""
        log_object = LogHandler('qa', 'host', 'test')
        self.assertEqual('qa', log_object.source_table)
#        self.assertEqual('host', log_object.host_name)


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
        col_obj = ColumnDDL('VENDR_NM', 'VARCHAR', '10,5')
        self.assertEqual(10, col_obj.data_len)
        self.assertEqual(10, col_obj.precision)
        self.assertEqual(5, col_obj.scale)


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
        self.assertEqual(self.srcObj.database_name,
                         self.source_database_name)
        self.assertEqual(self.srcObj.table_name, self.source_table_name)

    @patch('lib.ingest.pre_quality_assurance_export.ImpalaConnect',
           autospec=True)
    def test_get_source_row_count(self, mock_source_table):
        """test."""
        mock_source_table.run_query.return_value = [['10000']]
        self.assertEqual(self.srcObj.get_source_row_count(), 10000)

    def test_build_ddl_query(self):
        """test."""
        expected = "describe qa.fake_database_fake_cen_tablename;"
        self.assertEqual(self.srcObj.build_ddl_query(), expected)


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
        expected = "SELECT COUNT(*) FROM TEST_DB.TEST"
        test_output = self.ora_obj.build_row_count_query()
        self.assertEqual(expected, test_output)

    def test_clean_query_result(self):
        """test."""
        expected = ['| COUNT(*)             |', '| 10000                |']
        test_output = self.ora_obj._clean_query_result(self.row_count_expected)
        self.assertEqual(expected, test_output)

    def test_build_ddl_query(self):
        """test."""
        expected = ("SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH,"
                    " DATA_PRECISION, DATA_SCALE, NULLABLE, CHAR_LENGTH FROM"
                    " ALL_TAB_COLUMNS WHERE"
                    " OWNER='TEST_DB' AND TABLE_NAME='TEST'"
                    " ORDER BY column_id")
        self.assertEqual(self.ora_obj.build_ddl_query(), expected)

    def test_build_row_column_query(self):
        """test."""
        expected = "SELECT * FROM TEST_DB.TEST WHERE ROWNUM <= 3"
        test_output = self.ora_obj.build_row_column_query()
        self.assertEqual(expected, test_output)

    def test_build_increment_query(self):
        """test."""
        expected = ("SELECT COLUMN_NAME, TABLE_NAME, DATA_TYPE,"
                    " OWNER FROM all_tab_columns WHERE"
                    " owner = 'TEST_DB' AND "
                    "table_name = 'TEST' AND "
                    "identity_column = 'YES'")
        test_output = self.ora_obj.build_increment_query()
        self.assertEqual(expected, test_output)


class DB2TableTest(unittest.TestCase):
    """test."""

    def setUp(self):
        """Test setup."""
        params = {
            'domain': 'test_domain',
            'database': 'TEST_DB',
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
        self.db_obj = DB2Table(
            params['jars'], params['jdbc_url'], params['connection_factories'],
            params['user_name'], params['jceks'], params['password_alias'],
            self.source_table, params['target_schema'])

    def test_build_ddl_query(self):
        """test."""
        expected = ("SELECT NAME, COLTYPE, LENGTH,"
                    " SCALE, NULLS FROM SYSIBM.SYSCOLUMNS WHERE TBNAME ="
                    " 'TEST_TABLE_NAME' AND TBCREATOR = 'TEST_DB'"
                    " ORDER BY colno")
        self.assertEqual(self.db_obj.build_ddl_query(), expected)

    def test_build_row_column_query(self):
        """test."""
        expected = ("SELECT * FROM TEST_DB.TEST_TABLE_NAME "
                    "FETCH FIRST 3 ROWS ONLY")
        test_output = self.db_obj.build_row_column_query()
        self.assertEqual(expected, test_output)

    def test_build_increment_query(self):
        """test."""
        expected = ("SELECT NAME, TBNAME, TBCREATOR, IDENTITY FROM"
                    " SYSIBM.SYSCOLUMNS WHERE TBNAME = 'TEST_TABLE_NAME' AND"
                    " TBCREATOR = 'TEST_DB' AND IDENTITY = 'Y'")
        test_output = self.db_obj.build_increment_query()
        self.assertEqual(expected, test_output)

    def test_build_row_count_query(self):
        """test."""
        expected = "SELECT COUNT(*) FROM TEST_DB.TEST_TABLE_NAME"
        test_output = self.db_obj.build_row_count_query()
        self.assertEqual(expected, test_output)

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

    def test_build_ddl_query(self):
        """test."""
        expected = "HELP COLUMN TEST_TABLE_NAME.*;"
        self.assertEqual(self.td_obj.build_ddl_query(), expected)

    def test_build_row_column_query(self):
        """test."""
        expected = "SELECT TOP 3 * FROM TEST_DB.TEST_TABLE_NAME"
        test_output = self.td_obj.build_row_column_query()
        self.assertEqual(expected, test_output)

    def test_build_increment_query(self):
        """test."""
        expected = ("select ColumnName, IdColType, DatabaseName, TableName"
                    " from dbc.columns where DatabaseName='TEST_DB' and"
                    " TableName='TEST_TABLE_NAME' and IdColType in"
                    " ('GA', 'GD')")
        test_output = self.td_obj.build_increment_query()
        self.assertEqual(expected, test_output)

    def test_build_row_count_query(self):
        """test."""
        expected = "SELECT COUNT(*) FROM TEST_DB.TEST_TABLE_NAME"
        test_output = self.td_obj.build_row_count_query()
        self.assertEqual(expected, test_output)

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

    def test_build_ddl_query(self):
        """test."""
        expected = ("SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,"
                    " NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE FROM"
                    " INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG='test_db'"
                    " AND TABLE_NAME='TEST_TABLE_NAME' AND"
                    " TABLE_SCHEMA='dbo' ORDER BY ordinal_position")
        self.assertEqual(self.sql_obj.build_ddl_query(), expected)

    def test_build_row_column_query(self):
        """test."""
        expected = "SELECT TOP 3 * FROM [test_db].[dbo].[TEST_TABLE_NAME]"
        test_output = self.sql_obj.build_row_column_query()
        self.assertEqual(expected, test_output)

    def test_build_increment_query(self):
        """test."""
        expected = ("select name, * from sys.columns as c where"
                    " c.is_identity=1 and object_id=(select object_id "
                    "from sys.tables where name='TEST_TABLE_NAME')")
        test_output = self.sql_obj.build_increment_query()
        self.assertEqual(expected, test_output)

    def test_build_row_count_query(self):
        """test."""
        expected = "SELECT COUNT(*) FROM [test_db].[dbo].[TEST_TABLE_NAME]"
        test_output = self.sql_obj.build_row_count_query()
        self.assertEqual(expected, test_output)

    def tearDown(self):
        """test."""
        pass


class MySqlTableTest(unittest.TestCase):
    """test."""

    def setUp(self):
        """Test setup."""
        params = {
            'domain': 'test_domain',
            'database': 'test_db',
            'target_table': 'test_table_name',
            'target_dir': 'test_tar_dir',
            'jars': 'test_jars',
            'jdbc_url': 'jdbc:mysql://fake.test.sqlserver'
                        ':1433;database=test_db',
            'connection_factories': 'test_conn_fac',
            'user_name': 'test_username',
            'jceks': 'test_jceks',
            'password_alias': 'test_passwd',
            'target_host': 'test_t_host',
            'target_schema': 'dbo'
        }
        self.source_table = params['database'] + '.' + params['target_table']
        self.mysql_obj = MySqlTable(
            params['jars'], params['jdbc_url'], params['connection_factories'],
            params['user_name'], params['jceks'], params['password_alias'],
            self.source_table, params['target_schema'])

    def test_build_ddl_query(self):
        """test."""
        expected = ("SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,"
                    " NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE FROM"
                    " INFORMATION_SCHEMA.COLUMNS WHERE"
                    " TABLE_SCHEMA='test_db' AND"
                    " TABLE_NAME='test_table_name' ORDER"
                    " BY ordinal_position")
        self.assertEqual(self.mysql_obj.build_ddl_query(), expected)

    def test_build_row_column_query(self):
        """test."""
        expected = "SELECT * FROM dbo.test_table_name LIMIT 3"
        test_output = self.mysql_obj.build_row_column_query()
        self.assertEqual(expected, test_output)

    def test_build_increment_query(self):
        """test."""
        expected = ("SELECT COLUMN_NAME, TABLE_NAME, TABLE_SCHEMA,"
                    " TABLE_CATALOG FROM information_schema.COLUMNS WHERE"
                    " EXTRA = 'auto_increment' AND TABLE_SCHEMA = "
                    "'TEST_DB' AND TABLE_NAME='test_table_name'")
        test_output = self.mysql_obj.build_increment_query()
        self.assertEqual(expected, test_output)

    def test_build_row_count_query(self):
        """test."""
        expected = "SELECT COUNT(*) FROM test_db.test_table_name"
        test_output = self.mysql_obj.build_row_count_query()
        self.assertEqual(expected, test_output)

    def tearDown(self):
        """test."""
        pass


class TableValidationTest(unittest.TestCase):
    """test."""

    @patch('lib.ingest.pre_quality_assurance_export.logger')
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
            'target_schema': 'None'
        }
        self.table_val_obj = TableValidation(
            'text stage.db_table', 'domain.db_table', self.params)

    def tearDown(self):
        """test."""
        pass

    @patch('lib.ingest.pre_quality_assurance_export.logger')
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

    @patch('lib.ingest.pre_quality_assurance_export.ImpalaConnect',
           autospec=True)
    @patch.object(SqoopUtils, 'eval', autospec=True)
    @patch('lib.ingest.pre_quality_assurance_export.logger')
    @patch.object(TargetTable, 'get_target_row_count', return_value=0)
    @patch.object(TargetTable, 'get_target_increment_column',
                  return_value=False)
    @patch.object(MSSqlTable, 'check_ddl_null',
                  return_value=True)
    def test_start_data_sampling(self, m4, m5, m6, m_logger, m_eval,
                                 mock_impala_conn):
        """test data sampling qa for sqlserver"""
        _path = BASE_DIR + '/fixtures/qa_data_sample_ddl.txt'
        with open(_path, 'r') as file_handler:
            sqoop_ddl = file_handler.read()
        m_eval.side_effect = [(0, sqoop_ddl, ''), (0, sqoop_ddl, '')]
        mock_impala_conn.run_query.side_effect = [DATA_SAMPLE_HIVE_DDL_EXPORT]
        self.params['jdbc_url'] = ('jdbc:sqlserver://fake.sqlserver;database=FAKE_DATABASE')
        table_val_obj = TableValidation(
            'parquet_stage.db_table', 'domain.db_table', self.params)
        return_bool = table_val_obj.start_data_sampling()
        self.assertTrue(return_bool[0])

    @patch('lib.ingest.pre_quality_assurance_export.ImpalaConnect',
           autospec=True)
    @patch.object(SqoopUtils, 'eval', autospec=True)
    @patch('lib.ingest.pre_quality_assurance_export.logger')
    @patch.object(TargetTable, 'get_target_row_count', return_value=0)
    @patch.object(TargetTable, 'get_target_increment_column',
                  return_value=False)
    def test_start_data_sampling_mssql(self, m4, m5, m_logger, m_eval,
                                       mock_impala_conn):
        """test data sampling qa for sqlserver"""
        _path = BASE_DIR + '/fixtures/qa_data_sample_ddl.txt'
        with open(_path, 'r') as file_handler:
            sqoop_ddl = file_handler.read()
        m_eval.side_effect = [(0, sqoop_ddl, ''), (0, sqoop_ddl, '')]
        mock_impala_conn.run_query.side_effect = [DATA_SAMPLE_HIVE_DDL_EXPORT]
        self.params['jdbc_url'] = ('jdbc:mssql://fake.mssql;database=FAKE_DATABASE')
        table_val_obj = TableValidation(
            'parquet_stage.db_table', 'domain.db_table', self.params)
        return_bool = table_val_obj.start_data_sampling()
        self.assertTrue(return_bool[0])

    @patch('lib.ingest.pre_quality_assurance_export.ImpalaConnect',
           autospec=True)
    @patch.object(SqoopUtils, 'eval', autospec=True)
    @patch('lib.ingest.pre_quality_assurance_export.logger')
    @patch.object(TargetTable, 'get_target_row_count', return_value=0)
    @patch.object(TargetTable, 'get_target_increment_column',
                  return_value='data')
    @patch.object(TeraDataTable, 'check_ddl_null', return_value=True)
    def test_start_data_sampling_td(self, m4, m5, m6, m_logger, m_eval,
                                    mock_impala_conn):
        """test data sampling qa for terdata"""
        _path = BASE_DIR + '/fixtures/qa_data_sample_ddl_tera.txt'
        with open(_path, 'r') as file_handler:
            sqoop_ddl = file_handler.read()
        m_eval.side_effect = [(0, sqoop_ddl, ''), (0, sqoop_ddl, '')]
        mock_impala_conn.run_query.side_effect = [DATA_SAMPLE_HIVE_DDL_TERA]
        self.params['jdbc_url'] = ('jdbc:teradata://fake.teradata:1433;database=FAKE_DATABASE')
        table_val_obj = TableValidation(
            'parquet_stage.db_table', 'domain.db_table', self.params)
        return_bool = table_val_obj.start_data_sampling()
        self.assertTrue(return_bool[0])

    @patch('lib.ingest.pre_quality_assurance_export.ImpalaConnect',
           autospec=True)
    @patch.object(SqoopUtils, 'eval', autospec=True)
    @patch('lib.ingest.pre_quality_assurance_export.logger')
    @patch.object(TargetTable, 'get_target_increment_column',
                  return_value=False)
    @patch.object(TargetTable, 'get_target_row_count', return_value=0)
    @patch.object(OracleTable, 'check_ddl_null', return_value=True)
    def test_start_data_sampling_fal(self, m4, m5, m6, m_logger, m_eval,
                                     mock_impala_conn):
        """test data sampling qa for sqlserver."""
        _path = BASE_DIR + '/fixtures/qa_data_sample_oracle.txt'
        with open(_path, 'r') as file_handler:
            sqoop_ddl = file_handler.read()
        m_eval.side_effect = [(0, sqoop_ddl, ''), (0, sqoop_ddl, '')]
        mock_impala_conn.run_query.side_effect = [SAMPLE_HIVE_EXPORT_FALSE]
        self.params['jdbc_url'] = ('jdbc:oracle://fake.oracle;database=FAKE_DATABASE')
        table_val_obj = TableValidation(
            'parquet_stage.db_table', 'domain.db_table', self.params)
        return_bool = table_val_obj.start_data_sampling()
        self.assertFalse(return_bool[0])
        self.assertTrue(return_bool[1])

    @patch('lib.ingest.pre_quality_assurance_export.ImpalaConnect',
           autospec=True)
    @patch.object(SqoopUtils, 'eval', autospec=True)
    @patch('lib.ingest.pre_quality_assurance_export.logger')
    @patch.object(TargetTable, 'get_target_row_count', return_value=0)
    @patch.object(TargetTable, 'get_target_increment_column',
                  return_value=False)
    def test_start_data_sampling_sql(self, m4, m5, m_logger, m_eval,
                                     mock_impala_conn):
        """test data sampling qa for sqlserver"""
        _path = BASE_DIR + '/fixtures/qa_data_sample_ddl.txt'
        with open(_path, 'r') as file_handler:
            sqoop_ddl = file_handler.read()
        m_eval.side_effect = [(0, sqoop_ddl, ''),
                              (0, sqoop_ddl, '')]
        mock_impala_conn.run_query.side_effect = [SAMPLE_HIVE_EXPORT_FALSE]
        self.params['jdbc_url'] = ('jdbc:mysql://fake.mysql;database=FAKE_DATABASE')
        table_val_obj = TableValidation(
            'parquet_stage.db_table', 'domain.db_table', self.params)
        return_bool = table_val_obj.start_data_sampling()
        self.assertFalse(return_bool[0])

    @patch('lib.ingest.pre_quality_assurance_export.logger')
    def test_get_ddl_column(self, mock_logger):
        with open(BASE_DIR + '/fixtures/db2_ddl.txt', 'r') as file_handler:
            sqoop_output = file_handler.read()
        self.params['jdbc_url'] = ('jdbc:db2://fake.db2;database=FAKE_DATABASE')
        table_val_obj = TableValidation(
            'parquet_stage.db_table', 'domain.db_table', self.params)
        _, row_data = table_val_obj.target_obj.fetch_rows_sqoop(
            sqoop_output)
        with open(BASE_DIR + '/fixtures/db2_ddl_col.txt', 'r') as file_handler:
            sqoop_output = file_handler.read()
        self.params['jdbc_url'] = ('jdbc:db2://fake.db2;database=FAKE_DATABASE')
        table_val_obj = TableValidation(
            'parquet_stage.db_table', 'domain.db_table', self.params)
        column_labels, _ = table_val_obj.target_obj.fetch_rows_sqoop(
            sqoop_output)
        self.assertTrue(table_val_obj.ddl_column_matches(column_labels,
                                                         row_data))

    @patch('lib.ingest.pre_quality_assurance_export.logger')
    def test_get_ddl_column_mis(self, mock_logger):
        with open(BASE_DIR + '/fixtures/db2_ddl.txt', 'r') as file_handler:
            sqoop_output = file_handler.read()
        self.params['jdbc_url'] = ('jdbc:db2://fake.db2;database=FAKE_DATABASE')
        table_val_obj = TableValidation(
            'parquet_stage.db_table', 'domain.db_table', self.params)
        _, row_data = table_val_obj.target_obj.fetch_rows_sqoop(
            sqoop_output)
        with open(BASE_DIR + '/fixtures/db2_ddl_col_mis.txt', 'r') as file_r:
            sqoop_output = file_r.read()
        self.params['jdbc_url'] = ('jdbc:db2://fake.db2;database=FAKE_DATABASE')
        table_val_obj = TableValidation(
            'parquet_stage.db_table', 'domain.db_table', self.params)
        column_labels, _ = table_val_obj.target_obj.fetch_rows_sqoop(
            sqoop_output)
        self.assertFalse(table_val_obj.ddl_column_matches(column_labels,
                                                          row_data))

    @patch.object(TableValidation, "start_data_sampling",
                  return_value=[False, False, False])
    @patch.object(PyHDFS, "insert_update")
    @patch('lib.ingest.pre_quality_assurance_export.ImpalaConnect',
           autospec=True)
    @patch('os.environ', return_value='IMPALA_HOST')
    def test_insert_hive(self, m_impala_host, m_pyhdfs, m_sam, m_connect):
        args = """test source_database_name source_table_name database
        target_table jars jdbc_url connection_factories user_name
         jceks password_alias host_name target_schema False
          qa_exp_results_dir""".split()

        def getitem(index):
            return args[index]
        mock_sys = MagicMock()
        mock_sys.__getitem__.side_effect = getitem
        m_connect.invalidate_metadata.return_value = True
        with patch('sys.argv', mock_sys):
            with self.assertRaises(SystemExit):
                main()

    def test_compare_data_len(self):
        source_ddls = []
        source_ddls.append(ColumnDDL("ID", "String", "32,32"))
        source_ddls.append(ColumnDDL("name", "INTEGER", "4,4"))
        target_ddls = []
        target_ddls.append(ColumnDDL("ID", "String", "32,32"))
        target_ddls.append(ColumnDDL("name", "INTEGER", "4,4"))
        result = self.table_val_obj._compare_data_len(source_ddls, target_ddls)
        self.assertTrue(result[0])
        self.assertTrue(result[1])

    def test_compare_data_type(self):
        source_ddls = []
        source_ddls.append(ColumnDDL("ID", "String", "32,32"))
        source_ddls.append(ColumnDDL("name", "INT", "4,4"))
        target_ddls = []
        target_ddls.append(ColumnDDL("ID", "VARCHAR2", "32,32"))
        target_ddls.append(ColumnDDL("name", "INT", "4,4"))
        result = self.table_val_obj._compare_data_type(source_ddls,
                                                       target_ddls)
        self.assertTrue(result[0])
        self.assertFalse(result[1])

if __name__ == '__main__':
    unittest.main()
else:
    loader = unittest.TestLoader()

    test_classes_to_run = [SourceTableTest, TargetTableTest,
                           OracleTableTest, TeraDataTableTest, DB2TableTest,
                           MSSqlTableTest, TableValidationTest]
    # Create test suite
    qa_pre_test_suite_export = []
    for test_class in test_classes_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        qa_pre_test_suite_export.append(suite)
