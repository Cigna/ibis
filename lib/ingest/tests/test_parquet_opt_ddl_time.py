"""Tests for parquet_opt_ddl_time.py"""
import difflib
import os
import unittest

from mock import patch, MagicMock

from lib.ingest.parquet_opt_ddl_time import ConnectionManager, DDLTypes, main
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class ParquetOptTimeFunctionsTest(unittest.TestCase):

    """Tests."""

    def setUp(self):
        pass

    def tearDown(self):
        self.conn_mgr = None
        self.ddl_types = None

    def compare_xml(self, test_xml, expected_xml):
        """Compare two xml files."""
        same = True
        test_xml = [xml.strip().replace('\t', '') for xml in
                    test_xml.splitlines()]
        expected_xml = [xml.strip().replace('\t', '') for xml in
                        expected_xml.splitlines()]

        if "".join(expected_xml) != "".join(test_xml):
            same = False
            print ""
            print test_xml
            print expected_xml
            print "XML strings don't match."
            diff = difflib.unified_diff(test_xml, expected_xml)
            print '\n'.join(list(diff))
        return same

    @patch('lib.ingest.parquet_opt_ddl_time.ConnectionManager', autospec=True)
    @patch('lib.ingest.parquet_opt_ddl_time.sys', autospec=True)
    @patch('lib.ingest.parquet_opt_ddl_time.os', autospec=True)
    @patch('lib.ingest.parquet_opt_ddl_time.open')
    def test_main_full_load(self, m_open, m_os, m_sys, m_cm):
        """test main method"""
        m_sys.argv = ['test_arg0', 'test_arg1', 'test_arg2', 'test_arg3',
                      'test_arg4', 'full_load', 'test_arg6']
        cm_methods = MagicMock()
        cm_methods.create_ingest_table.return_value = 'test_ingest_hql'
        cm_methods.create_parquet_live.return_value = 'test_live_hql'
        cm_methods.get_hql.return_value = 'test_hql'
        cm_methods.get_incremental_hql.return_value = 'test_incr_hql'
        cm_methods.create_externaltable.return_value = \
            ('views', 'invalidate', 'info')
        m_cm.return_value = cm_methods
        main()
        self.assertEquals(m_open.call_count, 5)

        cm_methods.create_externaltable.return_value = ('', '', '')
        m_cm.return_value = cm_methods
        main()
        self.assertEquals(m_open.call_count, 7)

    @patch('lib.ingest.parquet_opt_ddl_time.ConnectionManager', autospec=True)
    @patch('lib.ingest.parquet_opt_ddl_time.sys', autospec=True)
    @patch('lib.ingest.parquet_opt_ddl_time.os', autospec=True)
    @patch('lib.ingest.parquet_opt_ddl_time.open')
    def test_main_incr(self, m_open, m_os, m_sys, m_cm):
        """test main method"""
        m_sys.argv = ['test_arg0', 'test_arg1', 'test_arg2', 'test_arg3',
                      'test_arg4', 'incremental', 'test_arg6']
        cm_methods = MagicMock()
        cm_methods.create_ingest_table.return_value = 'test_ingest_hql'
        cm_methods.create_parquet_live.return_value = 'test_live_hql'
        cm_methods.get_hql.return_value = 'test_hql'
        cm_methods.get_incremental_hql.return_value = 'test_incr_hql'
        cm_methods.create_externaltable.return_value = \
            ('views', 'invalidate', 'info')
        m_cm.return_value = cm_methods
        main()
        self.assertEquals(m_open.call_count, 5)

    @patch.object(ConnectionManager, 'sqoop_eval', autospec=True)
    @patch.object(ConnectionManager, 'get_schema', autospec=True)
    def test_get_full_hql(self, m_schema, m_sqoop_eval):
        """test full hql"""
        with open(BASE_DIR + '/fixtures/ddl_td_avro_parquet.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()
            m_sqoop_eval.return_value = sqoop_eval_output
        m_schema.return_value = DDLTypes(
            input_mapping=sqoop_eval_output, data_source="td",
            ingest_timestamp="2016-01-01 16:47:56")

        self.conn_mgr = ConnectionManager(
            'database_test', 'table_test', '', 'domain', 'jdbc_url_conn_test',
            'connect_factories', 'username', 'password', 'view', 'int',
            'domain', 'impala_host_name', '2016-01-01 16:47:56', 'hdfs_test',
            'jars_test', 'jdbc_test', 'ingestion')

        test_create_hql = self.conn_mgr.get_hql()
        with open(BASE_DIR + '/expected/full_create_table.hql', 'r') as file_h:
            expected_create_table_hql = file_h.read()
        self.assertTrue(self.compare_xml(expected_create_table_hql,
                                         test_create_hql))

    @patch.object(ConnectionManager, 'sqoop_eval', autospec=True)
    @patch.object(ConnectionManager, 'get_schema', autospec=True)
    def test_get_full_hql_for_incremental(self, m_schema, m_sqoop_eval):
        """test incremental hql"""
        with open(BASE_DIR + '/fixtures/ddl_td_avro_parquet.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()
            m_sqoop_eval.return_value = sqoop_eval_output

        m_schema.return_value = DDLTypes(
            input_mapping=sqoop_eval_output, data_source="td",
            ingest_timestamp="2016-01-01 16:47:56")

        self.conn_mgr = ConnectionManager(
            'database_test', 'table_test', '', 'domain', 'jdbc_url_conn_test',
            'connect_factories', 'username', 'password', 'view', 'int',
            'domain', 'impala_host_name', '2016-01-01 16:47:56', 'hdfs_test',
            'jars_test', 'jdbc_test', 'ingestion')

        test_create_hql = self.conn_mgr.get_hql("incremental")

        with open(BASE_DIR + '/expected/incremental_full_create_table.hql',
                  'r') as file_h:
            expected_create_table_hql = file_h.read()
        self.assertTrue(
            self.compare_xml(expected_create_table_hql, test_create_hql))

    @patch.object(ConnectionManager, 'sqoop_eval', autospec=True)
    @patch.object(ConnectionManager, 'get_schema', autospec=True)
    def test_get_incremental_hql(self, m_schema, m_sqoop_eval):
        """test incremental hql"""
        with open(BASE_DIR + '/fixtures/ddl_td_avro_parquet.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()
            m_sqoop_eval.return_value = sqoop_eval_output

        m_schema.return_value = DDLTypes(
            input_mapping=sqoop_eval_output, data_source="td",
            ingest_timestamp="2016-01-01 16:47:56")

        self.conn_mgr = ConnectionManager(
            'database_test', 'table_test', '', 'domain', 'jdbc_url_conn_test',
            'connect_factories', 'username', 'password', 'view', 'int',
            'domain', 'impala_host_name', '2016-01-01 16:47:56', 'hdfs_test',
            'jars_test', 'jdbc_test', 'ingestion')

        test_create_hql = self.conn_mgr.get_incremental_hql()

        with open(BASE_DIR + '/expected/incr_create_table.hql', 'r') as file_h:
            expected_create_table_hql = file_h.read()
        self.assertTrue(
            self.compare_xml(expected_create_table_hql, test_create_hql))

    def test_get_types_schema_mysql(self):
        """test avro parquet hql for mysql"""
        with open(BASE_DIR + '/fixtures/ddl_mysql.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()

        self.ddl_types = DDLTypes(input_mapping=sqoop_eval_output,
                                  data_source="mysql",
                                  ingest_timestamp="2016-01-01 16:47:56")

        with open(BASE_DIR + '/expected/mysql_avro_parquet_select.txt',
                  'r') as file_h:
            expected_select_hql = file_h.read()

        with open(BASE_DIR + '/expected/mysql_avro_parquet_create.txt',
                  'r') as file_h:
            expected_create_hql = file_h.read()

        test_select_hql, test_create_hql = self.ddl_types.create_ddl_mappings()
        self.assertTrue(self.compare_xml(test_select_hql, expected_select_hql))
        self.assertTrue(self.compare_xml(test_create_hql, expected_create_hql))

    def test_get_types_schema_ora(self):
        """test avro parquet hql for oracle"""
        with open(BASE_DIR + '/fixtures/ddl_ora_avro_parquet.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()
        self.ddl_types = DDLTypes(input_mapping=sqoop_eval_output,
                                  data_source="oracle",
                                  ingest_timestamp="2016-01-01 16:47:56")

        with open(BASE_DIR + '/expected/ora_avro_parquet_hql.txt',
                  'r') as file_h:
            expected_select_hql = file_h.read()

        with open(BASE_DIR + '/expected/ora_avro_parquet_create.txt',
                  'r') as file_h:
            expected_create_hql = file_h.read()

        test_select_hql, test_create_hql = self.ddl_types.create_ddl_mappings()
        self.assertTrue(self.compare_xml(expected_select_hql, test_select_hql))
        self.assertTrue(self.compare_xml(expected_create_hql, test_create_hql))

    def test_get_types_schema_db2(self):
        """test avro parquet hql for db2"""
        with open(BASE_DIR + '/fixtures/ddl_db2_avro_parquet.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()

        self.ddl_types = DDLTypes(input_mapping=sqoop_eval_output,
                                  data_source="db2",
                                  ingest_timestamp="2016-01-01 16:47:56")

        with open(BASE_DIR + '/expected/db2_avro_parquet_hql.txt',
                  'r') as file_h:
            expected_select_hql = file_h.read()

        with open(BASE_DIR + '/expected/db2_avro_parquet_create.txt',
                  'r') as file_h:
            expected_create_hql = file_h.read()

        test_select_hql, test_create_hql = self.ddl_types.create_ddl_mappings()
        self.assertTrue(self.compare_xml(test_select_hql, expected_select_hql))
        self.assertTrue(self.compare_xml(test_create_hql, expected_create_hql))

    def test_get_types_schema_td(self):
        """test avro parquet hql for td"""
        with open(BASE_DIR + '/fixtures/ddl_td_avro_parquet.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()

        self.ddl_types = DDLTypes(input_mapping=sqoop_eval_output,
                                  data_source="td",
                                  ingest_timestamp="2016-01-01 16:47:56")

        with open(BASE_DIR + '/expected/td_avro_parquet_hql.txt',
                  'r') as file_h:
            expected_select_hql = file_h.read()

        with open(BASE_DIR + '/expected/td_avro_parquet_create.txt',
                  'r') as file_h:
            expected_create_hql = file_h.read()
        test_select_hql, test_create_hql = self.ddl_types.create_ddl_mappings()
        self.assertTrue(self.compare_xml(expected_select_hql, test_select_hql))
        self.assertTrue(self.compare_xml(expected_create_hql, test_create_hql))

    def test_get_types_schema_mapping(self):
        with open(BASE_DIR + '/fixtures/ddl_td_avro_parquet.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()

        self.ddl_types = DDLTypes(input_mapping=sqoop_eval_output,
                                  data_source="td",
                                  ingest_timestamp="2016-01-01 16:47:56")

        td_column_type_1 = self.ddl_types.get_types_schema('A1')
        self.assertTrue(self.compare_xml(td_column_type_1, 'STRING'))

        td_column_type_2 = self.ddl_types.get_types_schema('TS')
        self.assertTrue(self.compare_xml(td_column_type_2, 'TIMESTAMP'))

        td_column_type_3 = self.ddl_types.get_types_schema('N')
        self.assertTrue(self.compare_xml(td_column_type_3, 'INT'))

        ora_column_type_1 = self.ddl_types.get_types_schema(
            'INTERVAL DAY(2) TO SECOND(6)')
        self.assertTrue(self.compare_xml(ora_column_type_1, 'STRING'))

        ora_column_type_2 = self.ddl_types.get_types_schema('URITYPE')
        self.assertTrue(self.compare_xml(ora_column_type_2, 'STRING'))

    def test_format_select(self):
        """test avro parquet hql for db2"""
        with open(BASE_DIR + '/fixtures/ddl_ora_avro_underscore.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()

        self.ddl_types = DDLTypes(input_mapping=sqoop_eval_output,
                                  data_source="oracle",
                                  ingest_timestamp="2016-01-01 16:47:56")

        row_with = "_testColumnName"
        row_without = "testColumnName"

        row_with_spaces = "row with spaces"

        int_expected = 'CAST(`i_testColumnName` AS INT) AS `_testColumnName`'
        string_expected = ('CAST(`testColumnName` AS STRING)'
                           ' AS `testColumnName`')
        timestamp_expected = "CAST(from_unixtime(unix_timestamp" \
                             "(`testColumnName`, " \
                             "'yyyy-MM-dd')) " \
                             "AS TIMESTAMP) " \
                             "AS `testColumnName`"
        string_spaces_exepected = "CAST(`rowwithspaces` AS STRING) " \
                                  "AS `rowwithspaces`"

        self.assertEqual(self.ddl_types.format_select(row_with, 'INT'),
                         int_expected)

        self.assertEqual(self.ddl_types.format_select(row_without, 'STRING'),
                         string_expected)

        self.assertEqual(
            self.ddl_types.format_select(row_without, 'TIMESTAMP',
                                         timestamp_format='yyyy-MM-dd'),
            timestamp_expected)

        self.assertEqual(
            self.ddl_types.format_select(row_with_spaces, 'STRING'),
            string_spaces_exepected)

    def test_remove_6_in_timestamp_columns(self):
        with open(BASE_DIR + '/fixtures/ddl_td_avro_parquet.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()

        self.ddl_types = DDLTypes(input_mapping=sqoop_eval_output,
                                  data_source="td",
                                  ingest_timestamp="2016-01-01 16:47:56")

        timestamp_column = "TIMESTAMP(6)"
        timestamp_column_standard = "TIMESTAMP"

        self.assertEqual(
            self.ddl_types.remove_6_in_timestamp_columns(timestamp_column),
            ['TIMESTAMP', '6'])
        self.assertEqual(self.ddl_types.remove_6_in_timestamp_columns(
            timestamp_column_standard), ['TIMESTAMP'])

    def test_func_of_all_special_types(self):
        with open(BASE_DIR + '/fixtures/ddl_td_avro_parquet.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()

        self.ddl_types = DDLTypes(input_mapping=sqoop_eval_output,
                                  data_source="td",
                                  ingest_timestamp="2016-01-01 16:47:56")

        function_names = self.ddl_types.func_of_all_special_types().keys()

        self.assertEqual(function_names,
                         ['CHAR', 'TIMESTAMP', 'DECIMAL', 'VARCHAR',
                          'TIMESTAMP_DATE'])

    def test_timestamp_date_td(self):
        with open(BASE_DIR + '/fixtures/ddl_td_avro_parquet.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()

        self.ddl_types = DDLTypes(input_mapping=sqoop_eval_output,
                                  data_source="td",
                                  ingest_timestamp="2016-01-01 16:47:56")

        timestamp_output = self.ddl_types.timestamp_date(
            ['', 'TEST_TIMESTAMP', 'AT', ''])

        self.assertEqual(timestamp_output,
                         "CAST(from_unixtime(unix_timestamp"
                         "(`TEST_TIMESTAMP`, 'yyyy-MM-dd')) "
                         "AS TIMESTAMP) AS `TEST_TIMESTAMP`")

    def test_timestamp_date_ora(self):
        with open(BASE_DIR + '/fixtures/ddl_td_avro_parquet.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()

        self.ddl_types = DDLTypes(input_mapping=sqoop_eval_output,
                                  data_source="oracle",
                                  ingest_timestamp="2016-01-01 16:47:56")

        timestamp_output = self.ddl_types.timestamp_date(
            ['', '_TEST_TIMESTAMP', 'TIMESTAMP', ''])
        timestamp_result = "CAST(from_unixtime(unix_timestamp" \
                           "(`i_TEST_TIMESTAMP`, 'yyyy-MM-dd HH:mm:ss')) " \
                           "AS TIMESTAMP) AS `_TEST_TIMESTAMP`"

        self.assertEqual(timestamp_output, timestamp_result)

    def test_timestamp_t_td(self):
        with open(BASE_DIR + '/fixtures/ddl_td_avro_parquet.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()

        self.ddl_types = DDLTypes(input_mapping=sqoop_eval_output,
                                  data_source="td",
                                  ingest_timestamp="2016-01-01 16:47:56")

        timestamp_output = self.ddl_types.timestamp_t(
            ['', 'TEST_TIMESTAMP', 'AT', ''])

        self.assertEqual(timestamp_output,
                         'CAST(`TEST_TIMESTAMP` AS TIMESTAMP) '
                         'AS `TEST_TIMESTAMP`')

    def test_char_t_td(self):
        with open(BASE_DIR + '/fixtures/ddl_td_avro_parquet.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()

        self.ddl_types = DDLTypes(input_mapping=sqoop_eval_output,
                                  data_source="td",
                                  ingest_timestamp="2016-01-01 16:47:56")

        char_output = self.ddl_types.char_t(
            ['', 'TEST_CHAR', 'CF', '', '', '5'])

        self.assertEqual(char_output,
                         'CAST(`TEST_CHAR` AS CHAR(5)) AS `TEST_CHAR`')

    def test_varchar_t_td(self):
        with open(BASE_DIR + '/fixtures/ddl_td_avro_parquet.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()

        self.ddl_types = DDLTypes(input_mapping=sqoop_eval_output,
                                  data_source="td",
                                  ingest_timestamp="2016-01-01 16:47:56")

        varchar_output = self.ddl_types.varchar_t(
            ['', 'TEST_VARCHAR', 'CV', '', '', '5'])

        self.assertEqual(
            varchar_output, 'CAST(`TEST_VARCHAR` AS VARCHAR(5))'
                            ' AS `TEST_VARCHAR`')

    def test_decimal_t_td(self):
        with open(BASE_DIR + '/fixtures/ddl_td_avro_parquet.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()

        self.ddl_types = DDLTypes(input_mapping=sqoop_eval_output,
                                  data_source="td",
                                  ingest_timestamp="2016-01-01 16:47:56")

        decimal_output = self.ddl_types.decimal_t(
            ['', 'TEST_DECIMAL', 'D', '', '', '5', '1', '2.5'])

        self.assertEqual(decimal_output,
                         'CAST(`TEST_DECIMAL` AS DECIMAL(1,2.5)) '
                         'AS `TEST_DECIMAL`')

    def test_convert_spl_chars(self):
        with open(BASE_DIR + '/fixtures/ddl_td_avro_parquet.txt',
                  'r') as file_h:
            sqoop_eval_output = file_h.read()

        self.ddl_types = DDLTypes(input_mapping=sqoop_eval_output,
                                  data_source="td",
                                  ingest_timestamp="2016-01-01 16:47:56")

        column_with_spaces = 'column with spaces'
        column_without_spaces = 'columnWithoutSpaces'
        removed_spaces = self.ddl_types.convert_spl_chars(column_with_spaces)
        not_removed = self.ddl_types.convert_spl_chars(column_without_spaces)

        self.assertEqual(removed_spaces, 'columnwithspaces')
        self.assertEqual(not_removed, column_without_spaces)

    @patch.object(ConnectionManager, 'get_schema', autospec=True)
    def test_create_externaltable(self, m_get_schema):
        """test incremental views"""
        jdbcurl = ('jdbc:sqlserver://fake.sqlserver:1433;database=fake_database;encrypt=true;'
                   'trustServerCertificate=true')

        m_get_schema.return_value = MagicMock(spec=DDLTypes)

        conn_mgr = ConnectionManager(
            'fake_database', 'mock_table', 'dbo', 'mock_domain', jdbcurl,
            'mock_connection_factories', 'mock_db_username',
            'mock_password_file', 'fake_view_open|fake_view_im', 'int', 'domain',
            'impala_host_name',
            '2016-01-01 16:47:56',
            'mock_hdfs_loc', 'jars_test', 'hive_jdbc_url', 'ingestion')
        conn_mgr.ddl_types.get_create_hql.return_value = 'mock_create_hql'
        view_hql, impl_txt, views_info_txt = \
            conn_mgr.create_externaltable('incremental')
        with open(BASE_DIR + '/expected/incremental_views_sqlserver.hql',
                  'r') as file_h:
            expected_view_hql = file_h.read()

        with open(BASE_DIR + '/expected/views_invalidate_sqlserver.txt',
                  'r') as file_h:
            expected_impl_txt = file_h.read()

        with open(BASE_DIR + '/expected/views_info_txt.txt', 'r') as file_h:
            expected_views_info_txt = file_h.read()

        self.assertTrue(self.compare_xml(expected_view_hql, view_hql))
        self.assertTrue(self.compare_xml(expected_impl_txt, impl_txt))
        self.assertTrue(self.compare_xml(
            expected_views_info_txt, views_info_txt))

    @patch.object(ConnectionManager, 'get_schema', autospec=True)
    @patch('lib.ingest.parquet_opt_ddl_time.ImpalaConnect', autospec=True)
    def test_create_externaltable_perf(self, mock_impala_conn, m_get_schema):
        """test  perf views with domain"""
        jdbcurl = ('jdbc:sqlserver://fake.sqlserver:1433;database=fake_database;encrypt=true;'
                   'trustServerCertificate=true')

        m_get_schema.return_value = MagicMock(spec=DDLTypes)
        mock_impala_conn.run_query.side_effect = [[['test0']],
                                                  None,
                                                  [['test0']],
                                                  None,
                                                  [['test0']],
                                                  None,
                                                  [['test0']],
                                                  None]

        conn_mgr = ConnectionManager(
            'fake_database', 'mock_table', 'dbo', 'mock_domain', jdbcurl,
            'mock_connection_factories', 'mock_db_username',
            'mock_password_file', 'fake_view_open|fake_view_im|pharmacy', 'PERF',
            'pharmacy', 'impala_host_name', '2016-01-01 16:47:56',
            'mock_hdfs_loc', 'jars_test', 'hive_jdbc_url', 'ingestion')
        conn_mgr.ddl_types.get_create_hql.return_value = 'mock_create_hql'
        view_hql, impl_txt, views_info_txt = \
            conn_mgr.create_externaltable('incremental')
        with open(BASE_DIR + '/expected/views_sqlserver_perf.hql',
                  'r') as file_h:
            expected_view_hql = file_h.read()

        with open(BASE_DIR + '/expected/views_invalidate_sqlserver_domain.txt',
                  'r') as file_h:
            expected_impl_txt = file_h.read()
        with open(BASE_DIR + '/expected/views_info_txt_domain.txt', 'r') as \
                file_h:
            expected_views_info_txt = file_h.read()

        self.assertTrue(self.compare_xml(expected_view_hql, view_hql))
        self.assertTrue(self.compare_xml(expected_impl_txt, impl_txt))
        self.assertTrue(self.compare_xml(
            expected_views_info_txt, views_info_txt))

    @patch.object(ConnectionManager, 'get_schema', autospec=True)
    @patch('lib.ingest.parquet_opt_ddl_time.ImpalaConnect', autospec=True)
    def test_create_externaltable_perf_wd(self, mock_impala_conn, m_get_schema):
        """test perf views without domain """
        jdbcurl = ('jdbc:sqlserver://fake.sqlserver:1433;database=fake_database;encrypt=true;'
                   'trustServerCertificate=true')

        m_get_schema.return_value = MagicMock(spec=DDLTypes)
        mock_impala_conn.run_query.side_effect = [[['test0']],
                                                  None,
                                                  [['test0']],
                                                  None,
                                                  [['test0']],
                                                  None,
                                                  [['test0']],
                                                  None]

        conn_mgr = ConnectionManager(
            'fake_database', 'mock_table', 'dbo', 'mock_domain', jdbcurl,
            'mock_connection_factories', 'mock_db_username',
            'mock_password_file', 'fake_view_open|fake_view_im', 'PERF', 'domain',
            'impala_host_name', '2016-01-01 16:47:56',
            'mock_hdfs_loc', 'jars_test', 'hive_jdbc_url', 'ingestion')
        conn_mgr.ddl_types.get_create_hql.return_value = 'mock_create_hql'
        view_hql, impl_txt, views_info_txt = \
            conn_mgr.create_externaltable('incremental')
        with open(BASE_DIR + '/expected/views_sqlserver_perf_nodomain.hql',
                  'r') as file_h:
            expected_view_hql = file_h.read()

        with open(BASE_DIR + '/expected/views_invalidate_sqlserver.txt',
                  'r') as file_h:
            expected_impl_txt = file_h.read()

        with open(BASE_DIR + '/expected/views_info_txt.txt', 'r') as file_h:
            expected_views_info_txt = file_h.read()

        self.assertTrue(self.compare_xml(expected_view_hql, view_hql))
        self.assertTrue(self.compare_xml(expected_impl_txt, impl_txt))
        self.assertTrue(self.compare_xml(
            expected_views_info_txt, views_info_txt))

    @patch.object(ConnectionManager, 'get_schema', autospec=True)
    @patch('lib.ingest.parquet_opt_ddl_time.ImpalaConnect', autospec=True)
    def test_create_externaltable_perf_fwd(self, mock_impala_conn, m_get_schema):
        """test perf views team frequency check without domain"""
        jdbcurl = ('jdbc:sqlserver://fake.sqlserver:1433;database=fake_database;encrypt=true;'
                   'trustServerCertificate=true')

        m_get_schema.return_value = MagicMock(spec=DDLTypes)
        mock_impala_conn.run_query.side_effect = [[['test0']],
                                                  [['mock_table']],
                                                  [['2017-07-23 17:41:40']],
                                                  [['daily', 'yes',
                                                    'mock_domain',
                                                    'fake_view_open.fake_database_mock_tbl']],
                                                  [['test0']],
                                                  [['mock_table']],
                                                  [['2017-07-23 17:41:40']],
                                                  [['daily', 'yes',
                                                    'mock_domain',
                                                    'fake_view_open.fake_database_mock_tbl']],
                                                  [['test0']],
                                                  [['mock_table']],
                                                  [['2017-07-23 17:41:40']],
                                                  [['daily', 'yes',
                                                    'mock_domain',
                                                    'fake_view_open.fake_database_mock_tbl']],
                                                  [['test0']],
                                                  [['mock_table']],
                                                  [['2017-07-23 17:41:40']],
                                                  [['daily', 'yes',
                                                    'mock_domain',
                                                    'fake_view_open.fake_database_mock_tbl']]]

        conn_mgr = ConnectionManager(
            'fake_database', 'mock_table', 'dbo', 'mock_domain', jdbcurl,
            'mock_connection_factories', 'mock_db_username',
            'mock_password_file', 'fake_view_open|fake_view_im', 'PERF',
            'domain', 'impala_host_name', '2016-01-01 16:47:56',
            'mock_hdfs_loc', 'jars_test', 'hive_jdbc_url', 'ingestion')
        conn_mgr.ddl_types.get_create_hql.return_value = 'mock_create_hql'
        view_hql, impl_txt, views_info_txt = \
            conn_mgr.create_externaltable('incremental')

        with open(BASE_DIR + '/expected/views_sqlserver_perf_nodomain_fwd.hql',
                  'r') as file_h:
            expected_view_hql = file_h.read()

        with open(BASE_DIR + '/expected/views_invalidate_sqlserver.txt',
                  'r') as file_h:
            expected_impl_txt = file_h.read()

        with open(BASE_DIR + '/expected/views_info_txt.txt', 'r') as file_h:
            expected_views_info_txt = file_h.read()

        self.assertTrue(expected_view_hql, view_hql)
        self.assertTrue(self.compare_xml(expected_impl_txt, impl_txt))
        self.assertTrue(self.compare_xml(
            expected_views_info_txt, views_info_txt))

    @patch.object(ConnectionManager, 'get_schema', autospec=True)
    @patch('lib.ingest.parquet_opt_ddl_time.ImpalaConnect', autospec=True)
    def test_create_externaltable_perf_fd(self, mock_impala_conn, m_get_schema):
        """test perf views team frequency check with domain"""
        jdbcurl = ('jdbc:sqlserver://fake.sqlserver:1433;database=fake_database;encrypt=true;'
                   'trustServerCertificate=true')

        m_get_schema.return_value = MagicMock(spec=DDLTypes)
        mock_impala_conn.run_query.side_effect = [
            [['test0']], [['mock_table']], [['2017-07-23 17:41:40']],
            [['daily', 'yes', 'mock_domain', 'fake_view_open.fake_database_mock_table']],
            [['test0']], [['mock_table']], [['2017-07-23 17:41:40']],
            [['daily', 'yes', 'mock_domain', 'fake_view_open.fake_database_mock_table']],
            [['test0']], [['mock_table']], [['2017-07-23 17:41:40']],
            [['daily', 'yes', 'mock_domain', 'fake_view_open.fake_database_mock_table']],
            [['test0']], [['mock_table']], [['2017-07-23 17:41:40']],
            [['daily', 'yes', 'mock_domain', 'fake_view_open.fake_database_mock_table']]]

        conn_mgr = ConnectionManager(
            'fake_database', 'mock_table', 'dbo', 'mock_domain', jdbcurl,
            'mock_connection_factories', 'mock_db_username',
            'mock_password_file', 'fake_view_open|fake_view_im|pharmacy', 'PERF',
            'pharmacy', 'impala_host_name', '2016-01-01 16:47:56',
            'mock_hdfs_loc', 'jars_test', 'hive_jdbc_url', 'ingestion')
        conn_mgr.ddl_types.get_create_hql.return_value = 'mock_create_hql'
        view_hql, impl_txt, views_info_txt = \
            conn_mgr.create_externaltable('incremental')

        with open(BASE_DIR + '/expected/views_sqlserver_perf_fd.hql',
                  'r') as file_h:
            expected_view_hql = file_h.read()

        with open(BASE_DIR + '/expected/views_invalidate_sqlserver_domain.txt',
                  'r') as file_h:
            expected_impl_txt = file_h.read()

        with open(BASE_DIR + '/expected/views_info_txt_domain.txt', 'r') as \
                file_h:
            expected_views_info_txt = file_h.read()

        self.assertTrue(expected_view_hql, view_hql)
        self.assertTrue(self.compare_xml(expected_impl_txt, impl_txt))
        self.assertTrue(self.compare_xml(
            expected_views_info_txt, views_info_txt))

    @patch.object(ConnectionManager, 'get_schema', autospec=True)
    @patch('lib.ingest.parquet_opt_ddl_time.ImpalaConnect', autospec=True)
    def test_create_externaltable_perf_fdm(self, mock_impala_conn, m_get_schema):
        """test perf views team frequency check monthly, full load
        with domain"""
        jdbcurl = ('jdbc:sqlserver://fake.sqlserver:1433;database=fake_database;encrypt=true;'
                   'trustServerCertificate=true')

        m_get_schema.return_value = MagicMock(spec=DDLTypes)
        mock_impala_conn.run_query.side_effect = [
            [['test0']], [['mock_table']], [['2017-07-23 17:41:40']],
            [['monthly', 'yes', 'mock_domain', 'fake_view_open.fake_database_mock_table']],
            [['test0']], [['mock_table']], [['2017-07-23 17:41:40']],
            [['monthly', 'yes', 'mock_domain', 'fake_view_open.fake_database_mock_table']],
            [['test0']], [['mock_table']], [['2017-07-23 17:41:40']],
            [['monthly', 'yes', 'mock_domain', 'fake_view_open.fake_database_mock_table']],
            [['test0']], [['mock_table']], [['2017-07-23 17:41:40']],
            [['monthly', 'yes', 'mock_domain', 'fake_view_open.fake_database_mock_table']]]

        conn_mgr = ConnectionManager(
            'fake_database', 'mock_table', 'dbo', 'mock_domain', jdbcurl,
            'mock_connection_factories', 'mock_db_username',
            'mock_password_file', 'fake_view_open|fake_view_im|pharmacy', 'PERF',
            'pharmacy', 'impala_host_name', '2016-01-01 16:47:56',
            'mock_hdfs_loc', 'jars_test', 'hive_jdbc_url', 'ingestion')
        conn_mgr.ddl_types.get_create_hql.return_value = 'mock_create_hql'
        view_hql, impl_txt, views_info_txt = \
            conn_mgr.create_externaltable('full_load')

        with open(BASE_DIR + '/expected/views_sqlserver_perf_fd.hql',
                  'r') as file_h:
            expected_view_hql = file_h.read()

        with open(BASE_DIR + '/expected/views_invalidate_sqlserver_domain.txt',
                  'r') as file_h:
            expected_impl_txt = file_h.read()

        with open(BASE_DIR + '/expected/views_info_txt_domain.txt', 'r') as \
                file_h:
            expected_views_info_txt = file_h.read()

        self.assertTrue(expected_view_hql, view_hql)
        self.assertTrue(self.compare_xml(expected_impl_txt, impl_txt))
        self.assertTrue(self.compare_xml(
            expected_views_info_txt, views_info_txt))

    @patch.object(ConnectionManager, 'get_schema', autospec=True)
    def test_create_externaltable_perf_domain(self, m_get_schema):
        """test perf views with only domain"""
        jdbcurl = ('jdbc:sqlserver://fake.sqlserver:1433;database=fake_database;encrypt=true;'
                   'trustServerCertificate=true')

        m_get_schema.return_value = MagicMock(spec=DDLTypes)

        conn_mgr = ConnectionManager(
            'fake_database', 'mock_table', 'dbo', 'mock_domain', jdbcurl,
            'mock_connection_factories', 'mock_db_username',
            'mock_password_file', 'pharmacy|benefits', 'PERF',
            'pharmacy,benefits,claim', 'impala_host_name',
            '2016-01-01 16:47:56',
            'mock_hdfs_loc', 'jars_test', 'hive_jdbc_url', 'ingestion')
        conn_mgr.ddl_types.get_create_hql.return_value = 'mock_create_hql'
        view_hql, impl_txt, views_info_txt = \
            conn_mgr.create_externaltable('full_load')

        with open(BASE_DIR + '/expected/views_sqlserver_perf_domain.hql',
                  'r') as file_h:
            expected_view_hql = file_h.read()

        with open(BASE_DIR +
                  '/expected/views_invalidate_sqlserver_only_domain.txt',
                  'r') as file_h:
            expected_impl_txt = file_h.read()

        with open(BASE_DIR + '/expected/views_info_txt_only_domain.txt', 'r')\
                as file_h:
            expected_views_info_txt = file_h.read()
        self.assertTrue(expected_view_hql, view_hql)
        self.assertTrue(self.compare_xml(expected_impl_txt, impl_txt))
        self.assertTrue(self.compare_xml(
            expected_views_info_txt, views_info_txt))

    @patch.object(ConnectionManager, 'get_schema', autospec=True)
    def test_create_externaltable_full(self, m_get_schema):
        """test full ingest views"""
        jdbcurl = ('jdbc:sqlserver://fake.sqlserver:1433;database=fake_database;encrypt=true;'
                   'trustServerCertificate=true')
        m_get_schema.return_value = MagicMock(spec=DDLTypes)

        conn_mgr = ConnectionManager(
            'fake_database', 'mock_table', 'dbo', 'mock_domain', jdbcurl,
            'mock_connection_factories', 'mock_db_username',
            'mock_password_file', 'fake_view_open|fake_view_im', 'int', 'domain',
            'impala_host_name', '2016-01-01 16:47:56',
            'mock_hdfs_loc', 'jars_test', 'hive_jdbc_url', 'ingestion')
        conn_mgr.ddl_types.get_create_hql.return_value = 'mock_create_hql'
        view_hql, impl_txt, views_info_txt = \
            conn_mgr.create_externaltable('full_load')

        with open(BASE_DIR + '/expected/views_sqlserver.hql',
                  'r') as file_h:
            expected_view_hql = file_h.read()

        with open(BASE_DIR + '/expected/views_invalidate_sqlserver.txt',
                  'r') as file_h:
            expected_impl_txt = file_h.read()

        with open(BASE_DIR + '/expected/views_info_txt.txt', 'r') as file_h:
            expected_views_info_txt = file_h.read()

        self.assertTrue(self.compare_xml(expected_view_hql, view_hql))
        self.assertTrue(self.compare_xml(expected_impl_txt, impl_txt))
        self.assertTrue(self.compare_xml(
            expected_views_info_txt, views_info_txt))

    @patch.object(ConnectionManager, 'get_schema', autospec=True)
    def test_create_externaltable_no_views(self, m_get_schema):
        """test no views"""
        jdbcurl = ('jdbc:sqlserver://fake.sqlserver:1433;database=fake_database;encrypt=true;'
                   'trustServerCertificate=true')

        m_get_schema.return_value = MagicMock(spec=DDLTypes)

        conn_mgr = ConnectionManager(
            'fake_database', 'mock_table', 'dbo', 'mock_domain', jdbcurl,
            'mock_connection_factories', 'mock_db_username',
            'mock_password_file', '', 'int', 'domain',
            'impala_host_name', '2016-01-01 16:47:56', 'mock_hdfs_loc',
            'jars_test', 'hive_jdbc_url', 'ingestion')
        conn_mgr.ddl_types.get_create_hql.return_value = 'mock_create_hql'
        view_hql, impl_txt, views_info_txt = \
            conn_mgr.create_externaltable('incremental')

        with open(BASE_DIR + '/expected/no_view_hql.txt', 'r') \
                as file_s:
            expected_view_hql = file_s.read()

        with open(BASE_DIR + '/expected/no_views_invalidate.txt', 'r') \
                as file_h:
            expected_impala_invalidate = file_h.read()

        with open(BASE_DIR + '/expected/no_views_info.txt', 'r') \
                as file_v:
            expected_views_info = file_v.read()

        self.assertEqual(view_hql, expected_view_hql)
        self.assertEqual(impl_txt, expected_impala_invalidate)
        self.assertEqual(views_info_txt, expected_views_info)

    @patch.object(ConnectionManager, 'get_schema', autospec=True)
    def test_create_ingest_table(self, m_get_schema):
        """test create_ingest_table"""
        jdbcurl = ('jdbc:oracle:thin:@//fake.oracle:1521'
                   '/fake_servicename')

        m_get_schema.return_value = MagicMock(spec=DDLTypes)

        conn_mgr = ConnectionManager(
            'fake_database', 'fake_$tablename', '', 'mock_domain', jdbcurl,
            'mock_connection_factories', 'mock_db_username',
            'mock_password_file', '', 'int', 'domain',
            'impala_host_name', '2016-01-01 16:47:56', 'mock_hdfs_loc',
            'jars_test', 'hive_jdbc_url', 'ingestion')
        test_hql = conn_mgr.create_ingest_table()
        with open(BASE_DIR + '/expected/create_ingest_table.hql', 'r') \
                as file_h:
            expected_hql = file_h.read()
        self.assertEqual(test_hql, expected_hql)

    @patch.object(ConnectionManager, 'get_schema', autospec=True)
    def test_create_parquet_live(self, m_get_schema):
        """test create_parquet_live"""
        jdbcurl = ('jdbc:oracle:thin:@//fake.oracle:1521'
                   '/fake_servicename')

        m_get_schema.return_value = MagicMock(spec=DDLTypes)

        conn_mgr = ConnectionManager(
            'fake_database', 'fake_$tablename', '', 'mock_domain', jdbcurl,
            'mock_connection_factories', 'mock_db_username',
            'mock_password_file', '', 'int', 'domain',
            'impala_host_name', '2016-01-01 16:47:56', 'mock_hdfs_loc',
            'jars_test', 'hive_jdbc_url', 'ingestion')
        test_hql = conn_mgr.create_parquet_live()
        with open(BASE_DIR + '/expected/create_parquet_live.hql', 'r') \
                as file_h:
            expected_hql = file_h.read()
        self.assertEqual(test_hql, expected_hql)

    @patch.object(ConnectionManager, 'get_schema', autospec=True)
    def test_build_ddl_query(self, m_get_schema):
        """test build_ddl_query"""
        m_get_schema.return_value = MagicMock(spec=DDLTypes)
        # sqlserver
        jdbcurl = ('jdbc:sqlserver://fake.sqlserver:1433;database=fake_database;encrypt=true;'
                   'trustServerCertificate=true')
        conn_mgr = ConnectionManager(
            'fake_database', 'mock_table', 'dbo', 'mock_domain', jdbcurl,
            'mock_connection_factories', 'mock_db_username',
            'mock_password_file', '', 'int', 'domain',
            'impala_host_name',
            '2016-01-01 16:47:56', 'mock_hdfs_loc',
            'jars_test', 'hive_jdbc_url', 'ingestion')
        test_query = conn_mgr.build_ddl_query()
        expected_query = \
            ("SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, "
             "NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE "
             "FROM INFORMATION_SCHEMA.COLUMNS"
             " WHERE TABLE_CATALOG='fake_database' AND TABLE_NAME='mock_table' AND "
             "TABLE_SCHEMA='dbo' ORDER BY ordinal_position")
        self.assertEqual(test_query, expected_query)
        # oracle
        jdbcurl = ('jdbc:oracle:thin:@//fake.oracle'
                   ':1521/fake_servicename')
        conn_mgr = ConnectionManager(
            'fake_database', 'mock_table', '', 'mock_domain', jdbcurl,
            'mock_connection_factories', 'mock_db_username',
            'mock_password_file', '', 'int', 'domain',
            'impala_host_name',
            '2016-01-01 16:47:56', 'mock_hdfs_loc',
            'jars_test', 'hive_jdbc_url', 'ingestion')
        test_query = conn_mgr.build_ddl_query()
        expected_query = \
            ("SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, "
             "DATA_SCALE, NULLABLE, CHAR_LENGTH FROM ALL_TAB_COLUMNS"
             " WHERE OWNER='FAKE_DATABASE' AND "
             "TABLE_NAME='MOCK_TABLE' ORDER BY column_id")
        self.assertEqual(test_query, expected_query)
        # db2
        jdbcurl = ('jdbc:db2://fake.db2:50200/fake_servicename')
        conn_mgr = ConnectionManager(
            'fake_database', 'mock_table', '', 'mock_domain', jdbcurl,
            'mock_connection_factories', 'mock_db_username',
            'mock_password_file', '', 'int', 'domain',
            'impala_host_name',
            '2016-01-01 16:47:56', 'mock_hdfs_loc',
            'jars_test', 'hive_jdbc_url', 'ingestion')
        test_query = conn_mgr.build_ddl_query()
        expected_query = \
            ("SELECT NAME, COLTYPE, LENGTH, SCALE, NULLS FROM "
             "SYSIBM.SYSCOLUMNS"
             " WHERE TBNAME = 'MOCK_TABLE' AND TBCREATOR = 'FAKE_DATABASE'"
             " ORDER BY colno")
        self.assertEqual(test_query, expected_query)
        # teradata
        jdbcurl = ('jdbc:teradata://fake.teradata/'
                   'database=fake_database')
        conn_mgr = ConnectionManager(
            'fake_database', 'mock_table', '', 'mock_domain', jdbcurl,
            'mock_connection_factories', 'mock_db_username',
            'mock_password_file', '', 'int', 'domain',
            'impala_host_name',
            '2016-01-01 16:47:56', 'mock_hdfs_loc',
            'jars_test', 'hive_jdbc_url', 'ingestion')
        test_query = conn_mgr.build_ddl_query()
        expected_query = "HELP COLUMN MOCK_TABLE.*;"
        self.assertEqual(test_query, expected_query)
        # mysql
        jdbcurl = ('jdbc:mysql://fake.mysql:3306/fake_servicename')
        conn_mgr = ConnectionManager(
            'fake_database', 'mock_table', '', 'mock_domain', jdbcurl,
            'mock_connection_factories', 'mock_db_username',
            'mock_password_file', '', 'int', 'domain',
            'impala_host_name',
            '2016-01-01 16:47:56', 'mock_hdfs_loc',
            'jars_test', 'hive_jdbc_url', 'ingestion')
        test_query = conn_mgr.build_ddl_query()
        expected_query = \
            ("SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, "
             "NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE"
             " FROM INFORMATION_SCHEMA.COLUMNS"
             " WHERE TABLE_SCHEMA='fake_database' AND TABLE_NAME='mock_table'"
             " ORDER BY ordinal_position")
        self.assertEqual(test_query, expected_query)


if __name__ == '__main__':
    unittest.main()
