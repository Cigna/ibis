"""Oozie action builder tests."""
import unittest
import os
import shutil
import difflib
from mock import patch
from ibis.inventor.action_builder import ActionBuilder
from ibis.inventor.dsl_parser import WorkflowRule
from ibis.utilities.config_manager import ConfigManager
from ibis.model.table import ItTable
from ibis.inventor.tests.fixture_workflow_generator import full_ingest_tbl, \
    fake_algnmt_tbl, full_ingest_tbl_custom_config, fake_fact_tbl_prop, \
    sqlserver_fake_tablename, td_fake_tablename, db2_fake_tablename, \
    fake_fact_tbl_prop_mysql, dollar_fake_tablename
from ibis.settings import UNIT_TEST_ENV


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

mock_view_dic = {'db': 'fake_database', 'tbl': 'fake_member_tablename',
                 'name': 'fake_view_im.fake_database_fake_member_tablename', 'select': '*', 'where': ''}


class ActionBuilderFunctionsTest(unittest.TestCase):
    """Tests the functionality of the ActionBuilder class."""

    @patch('ibis.inventory.inventory.Inventory._connect', autospec=True)
    def setUp(self, m):
        """Setup."""
        self.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        self.builder = ActionBuilder(self.cfg_mgr)
        self.cfg_mgr.hadoop_credstore_password_disable = False

    def compare_files(self, test_xml, expected_xml):
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
            diff = difflib.unified_diff(expected_xml, test_xml)
            print '\n'.join(list(diff))

        return same

    def test_gen_property_xml(self):
        """Test property xml generation."""
        xml = self.builder.gen_property_xml('oozie.launcher.action.main.class',
                                            'org.apache.oozie.action.'
                                            'hadoop.Hive2Main')
        expected = '<property>\n<name>oozie.' \
                   'launcher.action.main.class</name>\n' \
                   '<value>org.apache.oozie.action.hadoop.' \
                   'Hive2Main</value>\n</property>'
        self.assertTrue(self.compare_files(xml, expected))

    def test_gen_fork_xml(self):
        """Test fork xml generation."""
        gen_fork_concur = self.builder.gen_fork_xml("pipeline1",
                                                    ['tbl1', 'tbl2', 'tbl3',
                                                     'tbl4'], 'concurrent')
        expected = '\t<fork name=\"pipeline1\">\n\t\t' \
                   '<path start=\"tbl1_import_prep\"/>\n\t\t<path start=' \
                   '\"tbl2_import_prep\"/>\n\t\t<path ' \
                   'start=\"tbl3_import_prep\"/>\n\t\t<path ' \
                   'start=\"tbl4_import_prep' \
                   '\"/>\n\t</fork>'
        gen_fork_concur1 = self.builder.gen_fork_xml("pipeline1",
                                                     ['tbl1', 'tbl2'],
                                                     'concurrent')
        expected1 = '\t<fork name=\"pipeline1\">\n\t\t<path ' \
                    'start=\"tbl1_import_prep\"/>\n\t\t<path start=' \
                    '\"tbl2_import_prep\"/>\n\t</fork>'
        gen_fork_stag = self.builder.gen_fork_xml(
            "pipeline1", ['tbl1', 'tbl2'], 'staggered')
        expected2 = '\t<fork name=\"pipeline1\">\n\t\t<path ' \
                    'start=\"tbl1_avro\"/>\n\t\t<path start=' \
                    '\"tbl2_import_prep\"/>\n\t</fork>'
        self.assertTrue(self.compare_files(gen_fork_concur, expected))
        self.assertTrue(self.compare_files(gen_fork_concur1, expected1))
        self.assertTrue(self.compare_files(gen_fork_stag, expected2))

    def test_gen_join_xml(self):
        """Test join xml generation."""
        gen_join = self.builder.gen_join_xml("pipeline1_join", "pipeline2")
        expected = '<join name=\"pipeline1_join\" to=\"pipeline2\"/>'
        self.assertTrue(self.compare_files(gen_join, expected))

    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch('ibis.inventor.action_builder.ActionBuilder.get_col_types',
           autospec=True)
    def test_gen_import_incremental_lastmodified(self, m_get_col_types,
                                                 mock_eval):
        """Test the generation of the import action for various sources."""
        m_get_col_types.return_value = []
        mock_eval.return_value = [['table']]

        incremental = {
            'check_column': "test_col", 'incremental': "lastmodified",
            'last_value': "2012-10-11 11:11:11"}

        test_incr_query = "x > y"
        _tbl = {
            'domain': 'member',
            'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:'
                       '1521/fake_servicename',
            'db_username': 'fake_username',
            'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                             'fake.password.alias',
            'load': '000001', 'fetch_size': 50000, 'hold': 0,
            'source_database_name': 'fake_database', 'source_table_name': 'fake_mem_tablename'}
        self.builder.it_table = ItTable(_tbl, self.cfg_mgr)
        # ORACLE
        action = self.builder.gen_import_action(
            'fake_mem_tablename_import', incremental=incremental,
            sqoop_where_query=test_incr_query)
        action.ok = 'fake_mem_tablename_avro'
        oracle_xml = action.generate_action()
        _path = os.path.join(
            BASE_DIR, 'expected_workflows/oracle_incremental_sqoop.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(oracle_xml, expected))

    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch('ibis.inventor.action_builder.ActionBuilder.get_col_types',
           autospec=True)
    def test_gen_import_incremental_append(self, m_get_col_types, mock_eval):
        """Test the generation of the import action for various sources."""
        m_get_col_types.return_value = []
        mock_eval.return_value = [['table']]
        incremental = {
            'check_column': "test_col", 'incremental': "append",
            'last_value': "5555555"}

        _tbl = {
            'domain': 'member',
            'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
                       'fake_servicename',
            'db_username': 'fake_username',
            'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                             'fake.password.alias',
            'load': '000001', 'fetch_size': 50000, 'hold': 0,
            'source_database_name': 'fake_database', 'source_table_name': 'fake_mem_tablename'}
        self.builder.it_table = ItTable(_tbl, self.cfg_mgr)
        # ORACLE
        action = self.builder.gen_import_action(
            'fake_mem_tablename_import', incremental=incremental)
        action.ok = 'fake_mem_tablename_avro'
        oracle_xml = action.generate_action()
        _path = os.path.join(
            BASE_DIR, 'expected_workflows/oracle_incremental_append_sqoop.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(oracle_xml, expected))

    @patch('ibis.inventor.action_builder.ActionBuilder.get_col_types',
           autospec=True)
    def test_gen_import_incremental_failure(self, m_get_col_types):
        """Test the generation of the import action for various sources."""
        m_get_col_types.return_value = []
        incremental = {
            'check_column': "test_col", 'incremental': "lastmodified",
            'last_value': "5555555"}
        _tbl = {
            'domain': 'member',
            'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
                       'fake_servicename',
            'db_username': 'fake_username',
            'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                             'fake.password.alias',
            'load': '000001', 'fetch_size': 50000, 'hold': 0,
            'source_database_name': 'fake_database', 'source_table_name': 'fake_mem_tablename'}
        self.builder.it_table = ItTable(_tbl, self.cfg_mgr)
        # test invalid source
        with self.assertRaises(ValueError) as context:
            self.builder.gen_import_action(
                'fake_mem_tablename_import', incremental=incremental)

        self.assertTrue('Unknown incremental options' in
                        str(context.exception))

    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch('ibis.inventor.action_builder.ActionBuilder.get_col_types',
           autospec=True)
    def test_gen_import(self, m_get_col_types, mock_eval):
        """Test the generation of the import action for various sources."""
        m_get_col_types.return_value = [['caller_ty_id', 'NUMBER']]
        mock_eval.return_value = [['table']]

        _tbl = {
            'domain': 'member',
            'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
                       'fake_servicename',
            'db_username': 'fake_username',
            'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                             'fake.password.alias',
            'load': '000001', 'fetch_size': 50000, 'hold': 0,
            'source_database_name': 'fake_database', 'source_table_name': 'fake_mem_tablename'}
        self.builder.it_table = ItTable(_tbl, self.cfg_mgr)
        # ORACLE
        action = self.builder.gen_import_action('fake_mem_tablename_import')
        action.ok = 'fake_mem_tablename_avro'
        oracle_xml = action.generate_action()

        _path = os.path.join(BASE_DIR, 'expected_workflows/oracle_sqoop.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(oracle_xml, expected))

        _tbl = {
            'domain': 'member',
            'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
                       'fake_servicename',
            'db_username': 'fake_username',
            'password_file': '/user/dev/fake.password.file',
            'load': '000001', 'fetch_size': 50000, 'hold': 0,
            'source_database_name': 'fake_database', 'source_table_name': 'fake_mem_tablename'}
        self.builder.it_table = ItTable(_tbl, self.cfg_mgr)
        # ORACLE - PASSWORD-FILE
        action = self.builder.gen_import_action('fake_mem_tablename_import')
        action.ok = 'fake_mem_tablename_avro'
        oracle_pwdfile_xml = action.generate_action()

        _path = os.path.join(
            BASE_DIR, 'expected_workflows/oracle_action_import.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()

        self.assertTrue(self.compare_files(oracle_pwdfile_xml, expected))
        # Change .hadoop_credstore_password_disable to disable and add tag
        self.cfg_mgr.hadoop_credstore_password_disable = True

        _tbl = {
            'domain': 'member',
            'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
                       'fake_servicename',
            'db_username': 'fake_username',
            'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                             'fake.password.alias',
            'load': '000001', 'fetch_size': 50000, 'hold': 0,
            'source_database_name': 'fake_database', 'source_table_name': 'fake_mem_tablename'}
        self.builder.it_table = ItTable(_tbl, self.cfg_mgr)
        # ORACLE - PASSWORD-ALIAS
        action = self.builder.gen_import_action('fake_mem_tablename_import')
        action.ok = 'fake_mem_tablename_avro'
        oracle_xml = action.generate_action()

        _path = os.path.join(
            BASE_DIR, 'expected_workflows/oracle_sqoop_password_alias.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(oracle_xml, expected))

        _tbl = {
            'domain': 'call',
            'jdbcurl': 'jdbc:sqlserver://fake.sqlserver:1433;'
                       'database=FAKE_DATABASE',
            'db_username': 'fake_username', 'split_by': 'caller_ty_id',
            'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                             'fake.password.alias',
            'load': '000001', 'fetch_size': 50000, 'hold': 0, 'mappers': 1,
            'source_database_name': 'fake_database',
            'source_table_name': 'fake_call_tablename',
            'source_schema_name': 'dbo'}
        self.builder.it_table = ItTable(_tbl, self.cfg_mgr)
        # SQLSERVER and DB2 currently have the same settings
        action = self.builder.gen_import_action('fake_call_tablename_import')
        action.ok = 'fake_call_tablename_avro'
        sql_xml = action.generate_action()

        _path = os.path.join(
            BASE_DIR, 'expected_workflows/sqoop_sqlserver.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(sql_xml, expected))

        _tbl = {
            'domain': 'call',
            'jdbcurl': 'jdbc:sqlserver://fake.sqlserver:1433;'
                       'database=FAKE_DATABASE',
            'db_username': 'fake_username', 'split_by': 'caller_ty_id',
            'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                             'fake.password.alias',
            'load': '000001', 'fetch_size': 50000, 'hold': 0, 'mappers': 1,
            'source_database_name': 'fake_database',
            'source_table_name': 'fake_call_tablename',
            'source_schema_name': 'fake_warehouse_schema'}
        self.builder.it_table = ItTable(_tbl, self.cfg_mgr)
        # SQLSERVER and DB2 currently have the same settings
        action = self.builder.gen_import_action('fake_call_tablename_import')
        action.ok = 'fake_call_tablename_avro'
        sql_xml = action.generate_action()

        _path = os.path.join(
            BASE_DIR, 'expected_workflows/sqoop_sqlserver_non_dbo.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(sql_xml, expected))

        _tbl = {
            'domain': 'member',
            'jdbcurl': 'jdbc:teradata://fake.teradata/DATABASE='
                       'fake_database',
            'db_username': 'fake_username', 'split_by': 'caller_ty_id',
            'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                             'fake.password.alias',
            'load': '000001', 'fetch_size': 50000, 'hold': 0, 'mappers': 6,
            'source_database_name': 'fake_database',
            'source_table_name': 'fake_mth_tablename'}
        self.builder.it_table = ItTable(_tbl, self.cfg_mgr)
        m_get_col_types.return_value = [['caller_ty_id', 'TIMESTAMP']]

        # TERADATA
        action = self.builder.gen_import_action('fake_mth_tablename_import')
        action.ok = 'fake_mth_tablename_avro'
        td_xml = action.generate_action()
        _path = os.path.join(BASE_DIR, 'expected_workflows/teradata_sqoop.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(td_xml, expected))

    @patch('ibis.inventor.action_builder.ActionBuilder.get_col_types',
           autospec=True)
    def test_gen_import_custom_query(self, mock_get_col_types):
        """test user input - custom query"""
        mock_get_col_types.return_value = [('Col1', 'varchar'),
                                           ('Col2', 'DATETIME'),
                                           ('client_struc_key', 'INT')]
        _tbl = {
            'domain': 'member',
            'jdbcurl': 'jdbc:teradata://fake.teradata/DATABASE='
                       'fake_database',
            'db_username': 'fake_username', 'split_by': 'client_struc_key',
            'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                             'fake.password.alias',
            'load': '000001', 'fetch_size': 50000, 'hold': 0, 'mappers': 6,
            'source_database_name': 'fake_database',
            'source_table_name': 'fake_mth_tablename',
            'sql_query': "ID > 2 AND COL = 'TEST'"}
        self.builder.it_table = ItTable(_tbl, self.cfg_mgr)
        action = self.builder.gen_import_action(
            'fake_mth_tablename_import')
        action.ok = 'fake_mth_tablename_avro'
        td_xml = action.generate_action()
        _path = os.path.join(
            BASE_DIR, 'expected_workflows/sqoop_custom_query.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(expected, td_xml))

    @patch('ibis.inventor.action_builder.ActionBuilder.get_col_types',
           autospec=True)
    def test_gen_import_mysql(self, mock_get_col_types):
        """test mysql sqoop case sensitive"""
        mock_get_col_types.return_value = [('Col1', 'varchar'),
                                           ('Col2', 'DATETIME')]
        _tbl = {
            'domain': 'member',
            'jdbcurl': 'mysql://fake.mysql/ibis',
            'db_username': 'fake_username',
            'password_file': '/user/dev/fake.password.file',
            'load': '000001', 'fetch_size': 50000, 'hold': 0, 'views': 'fake_view_im',
            'source_database_name': 'fake_database',
            'source_table_name': 'MySqL_tesT'}
        self.builder.it_table = ItTable(_tbl, self.cfg_mgr)
        action = self.builder.gen_import_action(
            'MySqL_tesT_import')
        action.ok = 'MySqL_tesT_avro'
        mysql_xml = action.generate_action()
        _path = os.path.join(
            BASE_DIR, 'expected_workflows/sqoop_mysql.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(mysql_xml, expected))

    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch('ibis.inventor.action_builder.ActionBuilder.get_col_types',
           autospec=True)
    def test_gen_import_ora_xmltype(self, mock_get_col_types, mock_eval):
        """test oracle xmltype"""
        mock_get_col_types.return_value = [('Col1', 'varchar'),
                                           ('Col2', 'number'),
                                           ('Col3', 'xmltype'),
                                           ('Col4_#', 'xmltype')]
        mock_eval.return_value = [['table']]
        _tbl = {
            'domain': 'member',
            'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle'
                       ':1600/fake_servicename',
            'db_username': 'fake_username',
            'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                             'fake.password.alias',
            'load': '000001', 'fetch_size': 50000, 'hold': 0, 'mappers': 6,
            'source_database_name': 'fake_database',
            'source_table_name': 'fake_mth_tablename'}
        self.builder.it_table = ItTable(_tbl, self.cfg_mgr)
        action = self.builder.gen_import_action(
            'fake_mth_tablename_import')
        action.ok = 'fake_mth_tablename_avro'
        ora_xml = action.generate_action()
        _path = os.path.join(
            BASE_DIR, 'expected_workflows/ora_xmltype_sqoop.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(ora_xml, expected))

    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch('ibis.inventor.action_builder.ActionBuilder.get_col_types',
           autospec=True)
    def test_gen_import_ora_where(self, mock_get_col_types, mock_eval):
        """test oracle where clause"""
        mock_get_col_types.return_value = [('Col1', 'varchar'),
                                           ('Col2', 'number'),
                                           ('Col3', 'number'),
                                           ('Col4', 'number')]
        mock_eval.return_value = [['table']]
        _tbl = {
            'domain': 'member',
            'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle'
                       ':1600/fake_servicename',
            'db_username': 'fake_username',
            'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                             'fake.password.alias',
            'load': '000001', 'fetch_size': 50000, 'hold': 0, 'mappers': 6,
            'sql_query': 'ASSMT_DT_KEY > 1',
            'source_database_name': 'fake_database',
            'source_table_name': 'fake_mth_tablename'}
        self.builder.it_table = ItTable(_tbl, self.cfg_mgr)
        action = self.builder.gen_import_action(
            'fake_mth_tablename_import')
        action.ok = 'fake_mth_tablename_avro'
        ora_xml = action.generate_action()
        _path = os.path.join(
            BASE_DIR, 'expected_workflows/ora_sqoop_with_where.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(ora_xml, expected))

    def test_gen_avro_action(self):
        """Test the generation of the avro action xml."""
        self.builder.it_table = ItTable(fake_algnmt_tbl, self.cfg_mgr)
        action = self.builder.gen_avro_action('fake_algnmt_tablename_avro')
        action.ok = 'fake_algnmt_tablename_avro_parquet'
        avro_xml = action.generate_action()
        _path = os.path.join(BASE_DIR, 'expected_workflows/avro_action.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(avro_xml, expected))

    def test_gen_avro_parquet_action(self):
        """Test the generation of the avro parquet action xml."""
        self.builder.it_table = ItTable(fake_algnmt_tbl, self.cfg_mgr)
        action = self.builder.gen_avro_parquet_action(
            'fake_algnmt_tablename_avro_parquet')
        action.ok = 'fake_algnmt_tablename_parquet_swap'
        avro_par_xml = action.generate_action()
        _path = os.path.join(
            BASE_DIR, 'expected_workflows/avro_parquet_action.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(avro_par_xml, expected))

    def test_gen_avro_parquet_action_dev(self):
        """Tests the generation of the avro parquet action xml
        (hadoop credstore password enable)
        """
        self.cfg_mgr.hadoop_credstore_password_disable = True
        self.builder.it_table = ItTable(fake_algnmt_tbl, self.cfg_mgr)
        action = self.builder.gen_avro_parquet_action(
            'fake_algnmt_tablename_avro_parquet')
        action.ok = 'fake_algnmt_tablename_parquet_swap'
        avro_par_xml = action.generate_action()
        _path = os.path.join(
            BASE_DIR, 'expected_workflows/avro_parquet_action_dev.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(avro_par_xml, expected))

    def test_gen_parquet_clone_action(self):
        """Test generation of hive table cloning action."""
        gen_hive_xml = self.builder.gen_parquet_clone_action(
            'action_name', 'source_table', 'source_db', 'source_dir',
            'next_action', 'kill_action')
        _path = os.path.join(
            BASE_DIR, 'expected_workflows/hive_gen_action.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(gen_hive_xml, expected))

    def test_gen_sqoop_oracle_export_action(self):
        """Tests generation of oracle sqoop export"""
        # PASSWORD-FILE
        exec_sqoop_pf_xml = self.builder.gen_sqoop_oracle_export_action(
            'table_name', 'jdbc_url@1.1.1.1:1521:woot', 'src/dir',
            'src_table', 'update_key', 'target_db', 'target_table',
            'user', 'pass')
        _path = os.path.join(
            BASE_DIR, 'expected_workflows/sqoop_ora_exp_action_pf.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(exec_sqoop_pf_xml, expected))

        # PASSWORD-ALIAS
        exec_sqoop_pa_xml = self.builder.gen_sqoop_oracle_export_action(
            'table_name', 'jdbc_url@1.1.1.1:1521:woot', 'src/dir',
            'src_table', 'update_key', 'target_db', 'target_table', 'user',
            'jceks://hdfs/user/dev/fake.passwords.jceks#pass')
        with open(os.path.join(BASE_DIR,
                               'expected_workflows/sqoop_ora_exp_action.xml'),
                  'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(exec_sqoop_pa_xml, expected))

    def test_gen_sqoop_teradata_export_action(self):
        """Tests generation of teratata sqoop export"""
        # PASSWORD-FILE
        exec_sqoop_pf_xml = self.builder.gen_sqoop_teradata_export_action(
            'table_name', 'jdbc_url@1.1.1.1:1521:woot', 'src/dir', 'src_table',
            'target_db', 'target_table', 'user', 'pass')
        _path = os.path.join(
            BASE_DIR, 'expected_workflows/sqoop_tera_exp_action_pf.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(exec_sqoop_pf_xml, expected))

        # PASSWORD-ALIAS
        exec_sqoop_pa_xml = self.builder.gen_sqoop_teradata_export_action(
            'table_name', 'jdbc_url@1.1.1.1:1521:woot', 'src/dir',
            'src_table', 'target_db', 'target_table', 'user',
            'jceks://hdfs/user/dev/fake.passwords.jceks#pass')
        _path = os.path.join(
            BASE_DIR, 'expected_workflows/sqoop_tera_exp_action.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(exec_sqoop_pa_xml, expected))

    def test_gen_exec_hive_clone_action(self):
        """Test generation of hive table cloning action."""
        exec_hive_xml = self.builder.gen_exec_hive_clone_action(
            'action_name', 'source_dir', 'next_action', 'kill_action')
        _path = os.path.join(
            BASE_DIR, 'expected_workflows/hive_exec_action.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(exec_hive_xml, expected))

    def test_gen_parquet_clone_action_RDBMS(self):
        """Test generation of hive table cloning action."""
        gen_hive_xml = self.builder.gen_parquet_clone_action_RDBMS(
            'action_name', 'source_table', 'source_db', 'source_dir',
            'next_action', 'kill_action')
        _path = os.path.join(
            BASE_DIR, 'expected_workflows/hive_gen_action_db.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(gen_hive_xml, expected))

    def test_gen_sqoop_database_export_action(self):
        """Tests generation of oracle sqoop export"""
        # PASSWORD-FILE
        exec_sqoop_pf_xml = self.builder.gen_sqoop_database_export_action(
            'table_name', 'jdbc_url@1.1.1.1:1521:woot', 'src/dir', 'src_table',
            'update_key', 'target_db', 'target_table', 'user', 'pass', '10',
            'fake_staging_database')
        _path = os.path.join(
            BASE_DIR, 'expected_workflows/sqoop_ora_exp_action_pf_db.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(exec_sqoop_pf_xml, expected))

        # PASSWORD-ALIAS
        exec_sqoop_pa_xml = self.builder.gen_sqoop_database_export_action(
            'table_name', 'jdbc_url@1.1.1.1:1521:woot', 'src/dir', 'src_table',
            'update_key', 'target_db', 'target_table', 'user',
            'jceks://hdfs/user/dev/fake.passwords.jceks#pass', '10',
            'fake_staging_database')
        _path = os.path.join(
            BASE_DIR, 'expected_workflows/sqoop_ora_exp_action_db.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(exec_sqoop_pa_xml, expected))

    def test_gen_sqoop_database_export_action_without_staging(self):
        """Tests generation of oracle sqoop export"""
        # PASSWORD-FILE
        exec_sqoop_pf_xml = self.builder.gen_sqoop_database_export_action(
            'table_name', 'jdbc_url@1.1.1.1:1521:teradata', 'src/dir',
            'src_table', 'update_key', 'target_db', 'target_table', 'user',
            'pass', '10', '')
        _path = os.path.join(
            BASE_DIR, 'expected_workflows/sqoop_export_pf_td.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(exec_sqoop_pf_xml, expected))

        # PASSWORD-ALIAS
        exec_sqoop_pa_xml = self.builder.gen_sqoop_database_export_action(
            'table_name', 'jdbc_url@1.1.1.1:1521:teradata', 'src/dir',
            'src_table', 'update_key', 'target_db', 'target_table', 'user',
            'jceks://hdfs/user/dev/fake.passwords.jceks#pass', '10', '')
        _path = os.path.join(
            BASE_DIR, 'expected_workflows/sqoop_export_td.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(exec_sqoop_pa_xml, expected))

    def test_gen_sqoop_td_export_action(self):
        """Tests generation of TD sqoop export"""
        # PASSWORD-FILE
        exec_sqoop_pf_xml = self.builder.gen_sqoop_database_export_action(
            'table_name', 'jdbc_url:teradata@1.1.1.1:1521:woot', 'src/dir',
            'src_table', 'update_key', 'target_db', 'target_table', 'user',
            'pass', '10', 'fake_staging_database')
        _path = os.path.join(
            BASE_DIR, 'expected_workflows/sqoop_export_td_db.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(exec_sqoop_pf_xml, expected))

        # PASSWORD-ALIAS
        exec_sqoop_pa_xml = self.builder.gen_sqoop_database_export_action(
            'table_name', 'jdbc_url@1.1.1.1:1521:teradata', 'src/dir',
            'src_table', 'update_key', 'target_db', 'target_table', 'user',
            'jceks://hdfs/user/dev/fake.passwords.jceks#pass', '10',
            'fake_staging_database')
        _path = os.path.join(
            BASE_DIR, 'expected_workflows/sqoop_export_td.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(exec_sqoop_pa_xml, expected))

    def test_gen_exec_hive_clone_action_RDBMS(self):
        """Test generation of hive table cloning action."""
        exec_hive_xml = self.builder.gen_exec_hive_clone_action_RDBMS(
            'action_name', 'source_table', 'source_db', 'next_action',
            'kill_action')
        _path = os.path.join(
            BASE_DIR, 'expected_workflows/hive_exec_action_db.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(exec_hive_xml, expected))

    def test_gen_parquet_live_action(self):
        """Test the generation of the parquet live action xml."""
        self.builder.it_table = ItTable(fake_algnmt_tbl, self.cfg_mgr)
        action = self.builder.gen_parquet_live('fake_algnmt_tablename_parquet_live')
        action.ok = 'pipeline0_join'
        live_xml = action.generate_action()
        _path = os.path.join(
            BASE_DIR, 'expected_workflows/parquet_live_action.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(live_xml, expected))

    def test_gen_parquet_live_action_dev(self):
        """Tests the generation of the parquet live action xml
        (hadoop credstore password enable)"""
        self.cfg_mgr.hadoop_credstore_password_disable = True
        self.builder.it_table = ItTable(fake_algnmt_tbl, self.cfg_mgr)
        action = self.builder.gen_parquet_live('fake_algnmt_tablename_parquet_live')
        action.ok = 'pipeline0_join'
        live_xml = action.generate_action()
        _path = os.path.join(
            BASE_DIR, 'expected_workflows/parquet_live_action_dev.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(live_xml, expected))

    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch('ibis.inventor.action_builder.ActionBuilder.get_col_types',
           autospec=True)
    def test_gen_full_table_ingest(self, m_get_col_types, mock_eval):
        """Test the generation of all xml actions for a table ingest."""
        m_get_col_types.return_value = []
        mock_eval.return_value = [['table']]
        real_host = self.cfg_mgr.host
        self.builder.cfg_mgr.host = 'fake.workflow.host'
        # Expected workflows hardcoded w/ dev host
        table_obj = ItTable(full_ingest_tbl, self.cfg_mgr)
        ingest_xml = self.builder.gen_full_table_ingest(table_obj)
        self.builder.cfg_mgr.host = real_host  # Return host to real value
        path = os.path.join(BASE_DIR,
                            'expected_workflows/full_ingest_action.xml')
        with open(path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(ingest_xml, expected))

    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch('ibis.inventor.action_builder.ActionBuilder.get_col_types',
           autospec=True)
    def test_gen_full_view_ingest(self, m_get_col_types, mock_eval):
        """Test the generation of oracle xml actions for a view ingest."""
        m_get_col_types.return_value = []
        mock_eval.return_value = [['view']]
        real_host = self.cfg_mgr.host
        self.builder.cfg_mgr.host = 'fake.workflow.host'
        # Expected workflows hardcoded w/ dev host
        table_obj = ItTable(full_ingest_tbl, self.cfg_mgr)
        ingest_xml = self.builder.gen_full_table_ingest(table_obj)
        self.builder.cfg_mgr.host = real_host  # Return host to real value
        path = os.path.join(BASE_DIR,
                            'expected_workflows/full_view_ingest_action.xml')
        with open(path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(ingest_xml, expected))

    def test_gen_oracle_table_export(self):
        """Tests the generation of all actions for export to oracle"""
        # PASSWORD-FILE
        pwd_file_xml = self.builder.gen_oracle_table_export(
            'fake_database_fake_rule_tablename', 'member',
            '/user/data/mdm/member/fake_database/fake_rule_tablename_clone',
            'jdbc:oracle:thin:@//fake.oracle:'
            '1600/fake_servicename',
            'fake_rule_tablename_KEY', 'fake_rule_tablename', 'FAKE_SCHEMA', 'fake_username',
            'fake.password.alias')
        with open(os.path.join(BASE_DIR,
                               'expected_workflows/full_oracle_export_pf.xml'),
                  'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(pwd_file_xml, expected))

        # PASSWORD-ALIAS
        pwd_alias_xml = self.builder.gen_oracle_table_export(
            'fake_database_fake_rule_tablename', 'member',
            '/user/data/mdm/member/fake_database/fake_rule_tablename_clone',
            'jdbc:oracle:thin:@//fake.oracle:1600/'
            'fake_servicename',
            'RULE_KEY', 'fake_rule_tablename', 'FAKE_SCHEMA', 'fake_username',
            'jceks://hdfs/user/dev/fake.passwords.jceks#fake.password.alias')

        with open(os.path.join(BASE_DIR,
                               'expected_workflows/full_oracle_export.xml'),
                  'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(pwd_alias_xml, expected))

    def test_gen_refresh(self):
        """Test the generation of the refresh action."""
        self.builder.it_table = ItTable(full_ingest_tbl, self.cfg_mgr)
        action = self.builder.gen_refresh('fake_mem_tablename_refresh')
        action.ok = 'end'
        xml = action.generate_action()
        _path = os.path.join(BASE_DIR, 'expected_workflows/refresh.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(xml, expected))

    def test_get_sqoop_hives_credstore_config(self):
        """Test the config parameter generated for sqoop credentials with
        HADOOP_CREDSTORE_PASSWORD=non """
        password = 'jceks://hdfs/user/dev/fake.passwords.jceks'
        self.cfg_mgr.hadoop_credstore_password_disable = True
        config = self.builder.get_sqoop_credentials_config([], password)
        self.assertIn('oozie.launcher.mapred.map.child.env', str(config),
                      'This should be a password alias with hives '
                      'trustore disable = {0}'.
                      format(
                          str(self.cfg_mgr.hadoop_credstore_password_disable)))

    def test_no_get_hives_credstore_config(self):
        """Test the config parameter generated for sqoop credentials
        with HADOOP_CREDSTORE_PASSWORD=non """
        password = './user/dev/passwords.file'
        self.cfg_mgr.hadoop_credstore_password_disable = True
        config = self.builder.get_sqoop_credentials_config([], password)
        self.assertNotIn('oozie.launcher.mapred.map.child.env', str(config),
                         'This is a password file with hives '
                         'trustore disable = {0}'.
                         format(str(
                             self.cfg_mgr.hadoop_credstore_password_disable)))

    def test_gen_kite_ingest(self):
        """Test generating a kite_ingest action"""
        actual_xml = self.builder.gen_kite_ingest(
            'source_table_name', 'source_database_name',
            'hdfs_loc', 'ok', 'error')
        with open(os.path.join(BASE_DIR, 'expected_workflows/kite_ingest.xml'),
                  'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(actual_xml, expected))

    def test_gen_custom_workflow_scripts(self):
        """test gen_custom_workflow_scripts"""
        rule = WorkflowRule('hive_script')
        rule.custom_action_script = '/DEV/test/hive_test.hql'
        # set request_dir to files dir for testing sake
        self.cfg_mgr.requests_dir = self.cfg_mgr.files
        _path = os.path.join(self.cfg_mgr.files, 'DEV/test/')
        # create necessary dirs for testing
        os.makedirs(_path)
        _file_path = os.path.join(BASE_DIR, 'test_resources/hive_test.hql')
        shutil.copy(_file_path, _path)
        new_file = self.builder.gen_custom_workflow_scripts(
            rule, self.cfg_mgr.requests_dir)
        self.assertEquals(new_file, 'DEV_test_hive_test.hql')
        f_path = os.path.join(BASE_DIR, 'expected/custom_hive_script.hql')
        with open(f_path) as file_h:
            expected_hql = file_h.read()
        f_path = os.path.join(self.cfg_mgr.files, 'DEV_test_hive_test.hql')
        with open(f_path) as file_h:
            test_hql = file_h.read()
        self.assertTrue(self.compare_files(test_hql, expected_hql))

    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch('ibis.inventor.action_builder.ActionBuilder.get_col_types',
           autospec=True)
    def test_gen_full_table_ingest_custom_scripts(self, m_get_col_types,
                                                  mock_eval):
        """Test the generation of all xml actions for a table ingest
        when custom config scripts are provided."""
        m_get_col_types.return_value = [('trans_time', 'TIMESTAMP')]
        mock_eval.return_value = [['table']]
        real_host = self.cfg_mgr.host
        self.builder.cfg_mgr.host = 'fake.workflow.host'
        # setup alternative requests dir
        self.cfg_mgr.requests_dir = os.path.join(
            self.cfg_mgr.files, 'requests')
        os.makedirs(self.cfg_mgr.requests_dir)
        os.makedirs(os.path.join(self.cfg_mgr.requests_dir, 'DEV'))
        self.builder.dsl_parser.scripts_dir = self.cfg_mgr.requests_dir
        fixture_config_path = os.path.join(
            BASE_DIR, 'test_resources/custom_config_no_views.dsl')
        shutil.copy(fixture_config_path, self.cfg_mgr.requests_dir)
        fixture_hql_path = os.path.join(
            BASE_DIR, 'test_resources/hive_test.hql')
        shutil.copy(fixture_hql_path,
                    os.path.join(self.cfg_mgr.requests_dir, 'DEV'))
        fixture_sh_path = os.path.join(
            BASE_DIR, 'test_resources/shell_test.sh')
        shutil.copy(fixture_sh_path, self.cfg_mgr.requests_dir)
        # Expected workflows hardcoded w/ dev host
        table_obj = ItTable(full_ingest_tbl_custom_config, self.cfg_mgr)
        ingest_xml = self.builder.gen_full_table_ingest(table_obj)
        self.builder.cfg_mgr.host = real_host  # Return host to real value
        path = os.path.join(
            BASE_DIR, 'expected_workflows/'
                      'full_ingest_custom_config_scripts.xml')
        with open(path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(ingest_xml, expected))

    def test_get_sqoop_query(self):
        """test special chars"""
        ora_tbl = ItTable(fake_fact_tbl_prop, self.cfg_mgr)
        ora_dollar_tbl = ItTable(dollar_fake_tablename, self.cfg_mgr)
        sqls_tbl = ItTable(sqlserver_fake_tablename, self.cfg_mgr)
        td_tbl = ItTable(td_fake_tablename, self.cfg_mgr)
        db2_tbl = ItTable(db2_fake_tablename, self.cfg_mgr)
        col_types_ora = [['Col1', 'varchar'], ['Account_#', 'varchar'],
                         ['colu_', 'varchar']]
        col_types_ora_dollar = [['account', 'varchar']]
        col_types = [['Col1', 'varchar'], ['1name', 'varchar'],
                     ['Account_#', 'varchar'], ['cc-oun-t', 'varchar'],
                     ['o unt', 'varchar'],
                     ['_colu_', 'varchar']]

        ora_test = self.builder.get_sqoop_query(ora_tbl, col_types_ora)
        ora_dollar_test = self.builder.get_sqoop_query(
            ora_dollar_tbl, col_types_ora_dollar)
        sqls_test = self.builder.get_sqoop_query(sqls_tbl, col_types)
        td_test = self.builder.get_sqoop_query(td_tbl, col_types)
        db2_test = self.builder.get_sqoop_query(db2_tbl, col_types)

        ora_exp = ('SELECT Col1, "Account_#" AS Account_, '
                   'colu_ FROM fake_database.risk_fake_tablename t WHERE 1=1  '
                   'AND $CONDITIONS')
        ora_dollar_exp = ('SELECT "account" AS account FROM '
                          'fake_database.fake_$tablename t WHERE 1=1  AND $CONDITIONS')
        sqls_exp = ("SELECT Col1, [1name] AS [i_1name], "
                    "[Account_#] AS [Account_], "
                    "[cc-oun-t] AS [ccount], [o unt] AS [ount], "
                    "[_colu_] AS [i_colu_]"
                    " FROM [fake_database].[dbo].[sqlserver_fake_tablename] WHERE"
                    " 1=1  AND $CONDITIONS")
        td_exp = ("SELECT Col1, 1name AS i_1name, Account_# AS Account_, "
                  "cc-oun-t AS ccount, o unt AS ount, _colu_ AS i_colu_ "
                  "FROM FAKE_DATABASE.TD_FAKE_TABLENAME WHERE 1=1  AND $CONDITIONS")
        db2_exp = ("SELECT Col1, 1name AS i_1name, Account_# AS Account_, "
                   "cc-oun-t AS ccount, o unt AS ount, _colu_ AS i_colu_ "
                   "FROM FAKE_DATABASE.DB2_FAKE_TABLENAME WHERE 1=1  AND $CONDITIONS")

        self.assertEquals(ora_test[1], ora_exp)
        self.assertEquals(ora_dollar_test[1], ora_dollar_exp)
        self.assertEquals(sqls_test[1], sqls_exp)
        self.assertEquals(td_test[1], td_exp)
        self.assertEquals(db2_test[1], db2_exp)

    def test_add_map_column_java(self):
        """test add_map_column_java"""
        ora_tbl = ItTable(fake_fact_tbl_prop, self.cfg_mgr)
        sqls_tbl = ItTable(sqlserver_fake_tablename, self.cfg_mgr)
        td_tbl = ItTable(td_fake_tablename, self.cfg_mgr)
        db2_tbl = ItTable(db2_fake_tablename, self.cfg_mgr)
        mysql_tbl = ItTable(fake_fact_tbl_prop_mysql, self.cfg_mgr)

        col_types_ora = [['Col1', 'TIMESTAMP'], ['Account_#', 'TIMESTAMP'],
                         ['colu_', 'TIMESTAMP']]
        col_types = [['Col1', 'TIMESTAMP'], ['1name', 'TIMESTAMP'],
                     ['Account_#', 'TIMESTAMP'], ['cc-oun-t', 'TIMESTAMP'],
                     ['o unt', 'TIMESTAMP'],
                     ['_colu_', 'TIMESTAMP']]

        ora_test = self.builder.add_map_column_java(ora_tbl, col_types_ora)
        sqls_test = self.builder.add_map_column_java(sqls_tbl, col_types)
        td_test = self.builder.add_map_column_java(td_tbl, col_types)
        db2_test = self.builder.add_map_column_java(db2_tbl, col_types)
        mysql_test = self.builder.add_map_column_java(mysql_tbl, col_types)

        ora_exp = 'COL1=String,ACCOUNT_=String,COLU_=String'
        sqls_exp = ('Col1=String,i_1name=String,Account_=String,'
                    'ccount=String,ount=String,i_colu_=String')
        td_exp = ('Col1=String,i_1name=String,Account_=String,'
                  'ccount=String,ount=String,i_colu_=String')
        db2_exp = ('COL1=String,I_1NAME=String,ACCOUNT_=String,CCOUNT=String,'
                   'OUNT=String,I_COLU_=String')
        mysql_exp = ('Col1=String,i_1name=String,Account_=String,'
                     'ccount=String,ount=String,i_colu_=String')

        self.assertEquals(ora_test[1], ora_exp)
        self.assertEquals(sqls_test[1], sqls_exp)
        self.assertEquals(td_test[1], td_exp)
        self.assertEquals(db2_test[1], db2_exp)
        self.assertEquals(mysql_test[1], mysql_exp)

    def test__add_split_by(self):
        """test valid column split by"""
        td_tbl = ItTable(td_fake_tablename, self.cfg_mgr)
        self.builder.it_table = td_tbl
        with self.assertRaises(ValueError) as context:
            self.builder._add_split_by([], [['invalid_col', 'varchar']])
        msg = "Not a valid column name for split_by: 'fake_split_by'"
        self.assertTrue(msg in str(context.exception))

    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch('ibis.inventor.action_builder.ActionBuilder.get_col_types',
           autospec=True)
    def test_sql_windows_auth(self, m_get_col_types, mock_eval):
        """test sql windows authentication"""
        m_get_col_types.return_value = []
        mock_eval.return_value = [['table']]
        _tbl = {
            'domain': 'qa_domain',
            'jdbcurl': 'jdbc:jtds:sqlserver://fake.sqlserver:1433;'
                       'useNTLMv2=true;domain=fake_domain;database=qa_db',
            'db_username': 'qa_user',
            'password_file': 'jceks://hdfs/user/fake_username0/fake.passwords.jceks#'
                             'fake.password.alias',
            'mappers': 1,
            'load': '000001',
            'fetch_size': 5000,
            'hold': 0,
            'source_database_name': 'qa_db',
            'source_table_name': 'qa_tbl'}

        self.builder.it_table = ItTable(_tbl, self.cfg_mgr)
        action = self.builder.gen_import_action('qa_tbl_import')
        action.ok = 'qa_tbl_avro'
        sql_windows_xml = action.generate_action()
        _path = os.path.join(
            BASE_DIR, 'expected_workflows/sqlserver_windows_auth.xml')
        with open(_path, 'r') as my_file:
            expected = my_file.read()
        self.assertTrue(self.compare_files(sql_windows_xml, expected))

if __name__ == '__main__':
    unittest.main()
