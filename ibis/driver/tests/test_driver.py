"""Driver tests."""
import copy
import difflib
import os
import sys
import time
import unittest

from mock import patch, Mock, MagicMock

from ibis.driver.driver import Driver
from ibis.inventor.tests.fixture_workflow_generator import *
from ibis.inventory.cb_inventory import CheckBalancesInventory
from ibis.inventory.esp_ids_inventory import ESPInventory
from ibis.inventory.export_it_inventory import ExportITInventory
from ibis.inventory.inventory import Inventory
from ibis.inventory.it_inventory import ITInventory
from ibis.inventory.perf_inventory import PerfInventory
from ibis.inventory.request_inventory import Request, RequestInventory
from ibis.model.exporttable import ItTableExport
from ibis.model.table import ItTable
from ibis.settings import UNIT_TEST_ENV
from ibis.utilities.config_manager import ConfigManager
from ibis.utilities.file_parser import parse_file_by_sections
from ibis.utilities.it_table_generation import Get_Auto_Split
from ibis.utilities.utilities import Utilities
from ibis.utilities.vizoozie import VizOozie
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class DriverFunctionsTest(unittest.TestCase):

    """Tests the functionality of the Driver class."""

    @patch('ibis.driver.driver.Utilities', autospec=True)
    @patch.object(Inventory, '_connect', autospec=True)
    def setUp(self, mock_connect, m_U):
        """Setup."""
        mock_util_methods = MagicMock()
        mock_util_methods.run_subprocess = MagicMock()
        mock_util_methods.run_subprocess.return_value = 0
        m_U.return_value = mock_util_methods
        self.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        self.driver = Driver(self.cfg_mgr)
        self.start_time = time.time()

    def tearDown(self):
        """Tear down."""
        self.driver = None
        t2 = time.time() - self.start_time
        # print "%s: %.3f" % (self.id(), t2)

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

    def test_submit_it_file_empty(self):
        """Test submit it file with an empty file."""
        file_h = open(os.path.join(BASE_DIR, 'test_resources/empty_file.txt'),
                      'r')
        result = self.driver.submit_it_file(file_h)
        self.assertEquals(result, '')

    def test_submit_it_file_export_empty(self):
        """Test submit it file with an empty file."""
        file_h = open(os.path.join(BASE_DIR, 'test_resources/empty_file.txt'),
                      'r')
        result = self.driver.submit_it_file_export(file_h)
        self.assertEquals(result, '')

    @patch.object(Get_Auto_Split, 'get_split_by_column', return_value='')
    def test_submit_it_file_insert(self, m1):
        """Test submit it file with a valid it table file."""
        self.driver.req_inventory = MagicMock(spec=RequestInventory)
        self.driver.req_inventory.parse_file.return_value = \
            ([Request(mock_table_mapping_val, self.cfg_mgr)],
             'Parse File Success')
        self.driver.it_inventory.get_table_mapping = \
            MagicMock(spec=ITInventory.get_table_mapping)
        self.driver.it_inventory.get_table_mapping.return_value = {}
        self.driver.it_inventory.insert = MagicMock(spec=ITInventory.insert)
        self.driver.it_inventory.insert.return_value = (True, 'Insert Success')
        result = self.driver.submit_it_file('test')
        self.assertEquals(result, 'Parse File Success\nInsert Success')

    def test_submit_it_file_export_insert(self):
        """Test submit it file with a valid it table file."""
        self.driver.req_inventory = MagicMock(spec=RequestInventory)
        self.driver.req_inventory.parse_file_export.return_value = \
            ([Request(mock_table_mapping_val_export, self.cfg_mgr)],
             'Parse File Success')
        self.driver.export_it_inventory.get_table_mapping = \
            MagicMock(spec=ExportITInventory.get_table_mapping)
        self.driver.export_it_inventory.get_table_mapping.return_value = {}
        self.driver.export_it_inventory.insert_export = \
            MagicMock(spec=ExportITInventory.insert_export)
        self.driver.export_it_inventory.insert_export.return_value = \
            (True, 'Insert Success')
        result = self.driver.submit_it_file_export('test')
        self.assertEquals(result, 'Parse File Success\nInsert Success')

    @patch.object(Get_Auto_Split, 'get_split_by_column', return_value='')
    def test_submit_it_file_update(self, m1):
        """Test submit it file with an updated it table file."""
        self.driver.req_inventory = MagicMock(spec=RequestInventory)
        self.driver.req_inventory.parse_file.return_value = \
            ([Request(mock_table_mapping_val, self.cfg_mgr)],
             'Parse File Success')

        self.driver.it_inventory.insert = MagicMock(spec=ITInventory.insert)
        self.driver.it_inventory.update = MagicMock(spec=ITInventory.update)
        self.driver.it_inventory.get_table_mapping = MagicMock(
            spec=ITInventory.get_table_mapping)
        self.driver.it_inventory.update.return_value = (True, 'Update Success')
        updated_table = copy.deepcopy(mock_table_mapping_val)
        updated_table['db_username'] = 'updated_user'
        self.driver.it_inventory.get_table_mapping.return_value = updated_table
        result = self.driver.submit_it_file('test')
        self.assertEquals(result,
                          'Parse File Success\nUpdate Success')

    def test_submit_it_file_export_update(self):
        """Test submit it file with an updated it table file."""
        self.driver.req_inventory = MagicMock(spec=RequestInventory)
        self.driver.req_inventory.parse_file_export.return_value = \
            ([Request(mock_table_mapping_val_export, self.cfg_mgr)],
             'Parse File Success\nUpdate Success')

        self.driver.export_it_inventory.insert_export = \
            MagicMock(spec=ExportITInventory.insert_export)
        self.driver.export_it_inventory.update_export = \
            MagicMock(spec=ExportITInventory.update_export)
        self.driver.export_it_inventory.get_table_mapping = \
            MagicMock(spec=ExportITInventory.get_table_mapping)
        self.driver.export_it_inventory.update_export.return_value = \
            (True, 'Update Success')
        updated_table = copy.deepcopy(mock_table_mapping_val_export)
        updated_table['db_username'] = 'updated_user'
        self.driver.export_it_inventory.get_table_mapping.return_value = \
            updated_table
        result = self.driver.submit_it_file_export('test')
        self.assertEquals(result,
                          'Parse File Success\nUpdate Success')

    @patch('ibis.utilities.run_parallel.DryRunWorkflowManager.run_all',
           autospec=True)
    @patch('ibis.utilities.run_parallel.SqoopCacheManager.cache_ddl_views',
           autospec=True)
    @patch('ibis.utilities.run_parallel.SqoopCacheManager.cache_ddl_queries',
           autospec=True)
    @patch('ibis.driver.driver.Driver.update_it_table', autospec=True)
    @patch.object(sys, 'exit', autospec=True)
    @patch.object(Inventory, '_connect', autospec=True)
    @patch.object(Inventory, 'get_table_mapping',
                  return_value=mock_table_mapping_val)
    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch('ibis.driver.driver.RequestInventory.get_available_requests',
           autospec=True)
    def test_submit_request_valid(self, mock_get_available_requests, m_eval,
                                  m_get_t_m, mock_connect, mock_sys,
                                  m_s_it_file, m_sqoop_cache,
                                  m_sqoop_cache_view, m_dryrun):
        """Test submit request with a valid request file."""
        m_eval.return_value = [['Col1', 'TIMESTAMP'], ['Col2', 'TIMESTAMP'],
                               ['Col3', 'varchar']]
        mock_get_available_requests.return_value = \
            ([ItTable(mock_table_mapping_val, self.cfg_mgr)], [], [])
        # self.driver.it_inventory = MagicMock(spec=ITInventory)
        self.driver.vizoozie = MagicMock(spec=VizOozie)
        self.driver.req_inventory.it_inventory = MagicMock(spec=ITInventory)
        file_h = open(
            os.path.join(BASE_DIR, 'test_resources/request_test_valid.txt'),
            'r')
        result = self.driver.submit_request(file_h, True)
        self.assertIsNotNone(result)
        file_h = open(
            os.path.join(BASE_DIR, 'test_resources/request_test_valid.txt'),
            'r')
        result = self.driver.submit_request(file_h, False)
        self.assertIsNotNone(result)

    def test_submit_request_invalid(self):
        """Test submit request with an invalid request file."""
        file_h = open(
            os.path.join(BASE_DIR, 'test_resources/request_test_invalid.txt'),
            'r')
        self.assertRaises(ValueError, self.driver.submit_request, file_h, True)

    @patch('ibis.driver.driver.Driver.update_it_table', autospec=True)
    def test_submit_request_empty(self, m_s_it_file):
        """Test submit request with an empty file."""
        file_h = open(os.path.join(BASE_DIR, 'test_resources/empty_file.txt'),
                      'r')
        status, result = self.driver.submit_request(file_h, True)
        self.assertIn('Workflow not generated for request.', result)
        self.assertFalse(status)

    @patch.object(VizOozie, 'visualizeXML', autospec=True)
    @patch.object(VizOozie, 'convertDotToPDF', autospec=True)
    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch('ibis.utilities.run_parallel.DryRunWorkflowManager.run_all',
           autospec=True)
    @patch('ibis.utilities.run_parallel.SqoopCacheManager.cache_ddl_views',
           autospec=True)
    @patch('ibis.utilities.run_parallel.SqoopCacheManager.cache_ddl_queries',
           autospec=True)
    @patch('ibis.driver.driver.Driver.update_it_table', autospec=True)
    @patch.object(Inventory, '_connect', autospec=True)
    @patch('ibis.driver.driver.RequestInventory.get_available_requests',
           autospec=True)
    def test_submit_request_unavailable(self, m_get_ar, m_con,
                                        m_s_it_file, m_sqoop_cache,
                                        m_sqoop_cache_view,
                                        m_dryrun, m_eval, m_convert_pdf,
                                        m_vi_xml):
        """Test submit request."""
        m_eval.return_value = [['Col1', 'TIMESTAMP'], ['Col2', 'TIMESTAMP'],
                               ['Col3', 'varchar']]
        m_get_ar.side_effect = [
            ([ItTable(mock_table_mapping_val, self.cfg_mgr)],
             [ItTable(mock_table_mapping_val, self.cfg_mgr)], []),
            ([], [ItTable(mock_table_mapping_val, self.cfg_mgr)], [])]

        _path = os.path.join(BASE_DIR, 'test_resources/request_test_valid.txt')
        file_h = open(_path, 'r')
        _, result = self.driver.submit_request(file_h, True)
        self.assertIn('generated successfully', result)

        _path = os.path.join(BASE_DIR, 'test_resources/request_test_valid.txt')
        file_h = open(_path, 'r')
        _, result = self.driver.submit_request(file_h, True)
        self.assertIn('Workflow not generated for request.', result)

    def test_run_oozie_job(self):
        """Test run oozie."""
        self.driver.utilities = Mock(spec=Utilities)
        self.driver.utilities.run_workflow.return_value = True
        self.assertTrue(self.driver.run_oozie_job("test_xml"))

    def test_save_it_table(self):
        """Test save it table."""
        self.driver.it_inventory = Mock(spec=ITInventory)
        self.driver.it_inventory.save_all_tables.return_value = True
        self.assertTrue(self.driver.save_it_table(None))

    def test_update_lifespan(self):
        """Test update lifespan using a mocked table from
        check and balances table."""
        self.driver.cb_inventory = Mock(spec=CheckBalancesInventory)
        self.driver.cb_inventory.get.return_value = [
            ['directory', 'pull_time', 'avro_size',
             'ingest_timestamp', 'parquet_time',
             'parquet_size', 'rows', 'lifespan', 'ack',
             'cleaned', 'current_repull', 'domain',
             'table']]
        self.assertIn('updated with new lifespan in checks_balances',
                      self.driver.update_lifespan('db_name', 'tbl_name',
                                                  'lifespan'))

    def test_update_lifespan_no_tbl(self):
        """Test update lifespan using no table."""
        self.driver.cb_inventory = Mock(spec=CheckBalancesInventory)
        self.driver.cb_inventory.get.return_value = None
        self.assertIn('ecord doesn\'t exist for table=',
                      self.driver.update_lifespan('db_name', 'tbl_name',
                                                  'lifespan'))

    def test_update_all_lifespan(self):
        """Test update all lifespan."""
        self.driver.it_inventory = Mock(spec=ITInventory)
        self.driver.cb_inventory = Mock(spec=CheckBalancesInventory)
        self.driver.it_inventory.get_all_tables.return_value = [
            {'full_table_name': 'member.fake_database_fake_prog_tablename',
             'domain': 'member',
             'target_dir': 'mdm/member/fake_database/fake_prog_tablename',
             'split_by': '', 'mappers': 10, 'db_username': 'fake_username',
             'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:'
                        '1521/fake_servicename',
             'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
             'password_file': 'jceks://hdfs/user/dev/fake.passwords.'
                              'jceks#fake.password.alias',
             'load': '000100', 'fetch_size': 20000, 'hold': 0,
             'source_database_name': 'fake_database',
             'source_table_name': 'fake_prog_tablename', 'esp_appl_id': 'TEST01',
             'views': 'fake_view_im'},
            {'full_table_name': 'fake_domainfake_database_fake_job_tablename', 'domain': 'fake_domain',
             'target_dir': 'mdm/fake_domain/fake_database/fake_job_tablename', 'split_by': '',
             'mappers': 2,
             'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:'
                        '1521/fake_servicename',
             'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
             'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks'
                              '#fake.password.alias',
             'db_username': 'fake_username', 'load': '000100', 'fetch_size': 20000,
             'hold': 0,
             'source_database_name': 'fake_database',
             'source_table_name': 'fake_job_tablename',
             'esp_appl_id': 'TEST01', 'views': 'fake_view_im'}]
        self.driver.cb_inventory.get.return_value = [
            ['directory', 'pull_time', 'avro_size', 'ingest_timestamp',
             'parquet_time',
             'parquet_size', 'rows', 'lifespan', 'ack', 'cleaned',
             'current_repull',
             'domain', 'table']]
        self.assertIsNotNone(self.driver.update_all_lifespan())

    def test_update_all_lifespan_load_invalid(self):
        """ Tests update all lifespan using a mocked table and
        invalid load value """
        self.driver.it_inventory = Mock(spec=ITInventory)
        self.driver.it_inventory.get_all_tables.return_value = [
            {'full_table_name': 'member.fake_database_fake_prog_tablename',
             'domain': 'member',
             'target_dir': 'mdm/member/fake_database/fake_prog_tablename',
             'split_by': '',
             'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:'
                        '1521/fake_servicename',
             'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
             'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks'
                              '#fake.password.alias',
             'load': '200100', 'db_username': 'fake_username', 'mappers': 10,
             'fetch_size': 20000,
             'hold': 0, 'source_database_name': 'fake_database',
             'esp_appl_id': 'TEST01',
             'source_table_name': 'fake_prog_tablename', 'views': 'fake_view_im'}]
        self.assertEquals(self.driver.update_all_lifespan(), '')

    def test_update_all_lifespan_no_tables(self):
        """ Tests update all lifespan using no tables """
        self.driver.it_inventory = Mock(spec=ITInventory)
        self.driver.it_inventory.get_all_tables.return_value = []
        self.assertEquals(self.driver.update_all_lifespan(), '')

    @patch.object(VizOozie, 'visualizeXML', autospec=True)
    @patch.object(VizOozie, 'convertDotToPDF', autospec=True)
    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch('ibis.inventory.inventory.Inventory._connect', autospec=True)
    def test_gen_schedule_request(self, m_connect, m_eval, m_convert_pdf,
                                  m_vi_xml):
        """test gen wf for prod workflows"""
        m_eval.return_value = [['Col1', 'TIMESTAMP'], ['Col2', 'TIMESTAMP'],
                               ['Col3', 'varchar']]
        tables = [ItTable(heavy_3_prop, self.cfg_mgr)]
        gen_files = self.driver.gen_schedule_request(tables, 'test_wf',
                                                     'test_appl')
        self.assertEquals(len(gen_files), 5)
        self.assertIn('test_wf.xml', gen_files)
        self.assertIn('test_wf.ksh', gen_files)
        self.assertIn('test_wf_job.properties', gen_files)

    @patch('ibis.utilities.run_parallel.DryRunWorkflowManager.run_all',
           autospec=True)
    @patch('ibis.utilities.run_parallel.SqoopCacheManager.cache_ddl_views',
           autospec=True)
    @patch('ibis.utilities.run_parallel.SqoopCacheManager.cache_ddl_queries',
           autospec=True)
    @patch('ibis.utilities.utilities.Utilities.put_dry_workflow',
           autospec=True)
    @patch('ibis.utilities.utilities.Utilities.dryrun_workflow',
           autospec=True)
    @patch.object(ITInventory, 'get_all_tables_for_esp', autospec=True)
    @patch.object(ESPInventory, 'get_tables_by_id', autospec=True)
    @patch('ibis.inventory.inventory.Inventory._connect', autospec=True)
    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch.object(VizOozie, 'visualizeXML', autospec=True)
    @patch.object(VizOozie, 'convertDotToPDF', autospec=True)
    @patch.object(PerfInventory, 'insert_freq_ingest', autospec=True)
    def test_gen_prod_workflow(self, m_freq_ingest, m_convert_pdf,
                               m_v_xml, m_eval, m_c,
                               m_get_id, m_get_t_esp, m_dryrun,
                               m_put_w, m_sqoop_cache, m_sqoop_cache_view,
                               m_dryrun_all):
        """ Tests generate_prod_workflows with 3 tables. One light,
        one medium and oen heavy."""
        m_eval.return_value = [['Col1', 'varchar'], ['Col2', 'varchar']]
        m_get_id.side_effect = [appl_ref_id_tbl_01, appl_ref_id_tbl_02]
        _mock_esp_tables_02 = [ItTable(tbl, self.cfg_mgr) for tbl in
                               mock_esp_tables_02]
        m_get_t_esp.return_value = _mock_esp_tables_02
        self.cfg_mgr.env = 'perf'
        status, msg, git_files = self.driver.gen_prod_workflow('FAKED001')
        for file_name in git_files:
            git_file = 'full_fake_open_fake_prog_tablename.hql'
            if git_file in file_name:
                actual_hql_nm = os.path.join(self.cfg_mgr.files, file_name)
                with open(actual_hql_nm, 'r') as file_h:
                    actual_hql = file_h.read()
                with open(BASE_DIR + '/test_resources/git_team_hql.hql',
                          'r') as file_h:
                    expected_hql = file_h.read()
                self.assertTrue(expected_hql, actual_hql)
        self.assertEquals(len(git_files), 26)
        self.assertIn('Generated', msg)
        self.assertIn('workflow:', msg)
        self.assertIn('subworkflow:', msg)
        self.assertTrue(status)

    @patch('ibis.utilities.run_parallel.DryRunWorkflowManager.run_all',
           autospec=True)
    @patch('ibis.utilities.run_parallel.SqoopCacheManager.cache_ddl_views',
           autospec=True)
    @patch('ibis.utilities.run_parallel.SqoopCacheManager.cache_ddl_queries',
           autospec=True)
    @patch('ibis.utilities.utilities.Utilities.put_dry_workflow',
           autospec=True)
    @patch('ibis.utilities.utilities.Utilities.dryrun_workflow',
           autospec=True)
    @patch.object(ITInventory, 'get_all_tables_for_esp', autospec=True)
    @patch.object(ESPInventory, 'get_tables_by_id', autospec=True)
    @patch('ibis.inventory.inventory.Inventory._connect', autospec=True)
    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch.object(VizOozie, 'visualizeXML', autospec=True)
    @patch.object(VizOozie, 'convertDotToPDF', autospec=True)
    @patch.object(PerfInventory, 'insert_freq_ingest', autospec=True)
    def test_gen_prod_workflow_perf_nodomain(self, m_freq_ingest, m_convert_pdf,
                                            m_v_xml, m_eval, m_c,
                                            m_get_id, m_get_t_esp,
                                            m_dryrun, m_put_w, m_sqoop_cache,
                                            m_sqoop_cache_view, m_dryrun_all):
        """ Tests generate_prod_workflows with 3 tables. One light,
        one medium and oen heavy."""
        m_eval.return_value = [['Col1', 'varchar'], ['Col2', 'varchar']]
        m_get_id.side_effect = [appl_ref_id_tbl_01, appl_ref_id_tbl_02]
        _mock_esp_tables = [ItTable(tbl, self.cfg_mgr) for tbl in
                            mock_esp_tbl_perf_domain]
        m_get_t_esp.return_value = _mock_esp_tables
        self.cfg_mgr.env = 'perf'
        status, msg, git_files = self.driver.gen_prod_workflow('FAKED001')
        for file_name in git_files:
            git_file = 'full_fake_open_fake_prog_tablename.hql'
            if git_file in file_name:
                actual_hql_nm = os.path.join(self.cfg_mgr.files, file_name)

                with open(actual_hql_nm, 'r') as file_h:
                    actual_hql = file_h.read()
                with open(BASE_DIR +
                          '/test_resources/git_team_hql_nodomain.hql',
                          'r') as file_h:
                    expected_hql = file_h.read()
                self.assertTrue(expected_hql, actual_hql)
        self.assertEquals(len(git_files), 23)
        self.assertIn('Generated', msg)
        self.assertIn('workflow:', msg)
        self.assertIn('subworkflow:', msg)
        self.assertTrue(status)

    @patch('ibis.utilities.run_parallel.DryRunWorkflowManager.run_all',
           autospec=True)
    @patch('ibis.utilities.run_parallel.SqoopCacheManager.cache_ddl_views',
           autospec=True)
    @patch('ibis.utilities.run_parallel.SqoopCacheManager.cache_ddl_queries',
           autospec=True)
    @patch('ibis.utilities.utilities.Utilities.put_dry_workflow',
           autospec=True)
    @patch('ibis.utilities.utilities.Utilities.dryrun_workflow', autospec=True)
    @patch.object(ITInventory, 'get_all_tables_for_esp', autospec=True)
    @patch.object(ESPInventory, 'get_tables_by_id', autospec=True)
    @patch('ibis.inventory.inventory.Inventory._connect', autospec=True)
    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch.object(VizOozie, 'visualizeXML', autospec=True)
    @patch.object(VizOozie, 'convertDotToPDF', autospec=True)
    @patch.object(Driver, 'gen_incr_workflow_files', autospec=True)
    @patch.object(PerfInventory, 'insert_freq_ingest', autospec=True)
    def test_gen_prod_workflow_2(self, m_freq_ingest, m_gen_incr,
                                 m_convert_pdf, m_v_xml,
                                 m_eval, m_c, m_get_id, m_get_t_esp,
                                 m_dryrun, m_put_w, m_sqoop_cache,
                                 m_sqoop_cache_view, m_dryrun_all):
        """ Tests generate_prod_workflows with five tables. 3 heavy,
         one medium and one light."""
        m_eval.return_value = [['Col1', 'varchar'], ['Col2', 'varchar']]
        m_get_id.side_effect = [appl_ref_id_tbl_01, appl_ref_id_tbl_02]
        _mock_esp_tables_03 = [ItTable(tbl, self.cfg_mgr) for tbl in
                               mock_esp_tables_03]
        m_get_t_esp.return_value = _mock_esp_tables_03
        m_gen_incr.return_value = []
        self.cfg_mgr.env = 'perf'
        status, msg, git_files = self.driver.gen_prod_workflow('FAKED001')
        self.assertEquals(len(git_files), 40)
        self.assertIn('Generated', msg)
        self.assertIn('workflow:', msg)
        self.assertIn('subworkflow:', msg)
        self.assertTrue(status)

    @patch('ibis.utilities.run_parallel.DryRunWorkflowManager.run_all',
           autospec=True)
    @patch('ibis.utilities.run_parallel.SqoopCacheManager.cache_ddl_views',
           autospec=True)
    @patch('ibis.utilities.run_parallel.SqoopCacheManager.cache_ddl_queries',
           autospec=True)
    @patch('ibis.utilities.utilities.Utilities.put_dry_workflow',
           autospec=True)
    @patch('ibis.utilities.utilities.Utilities.dryrun_workflow', autospec=True)
    @patch.object(ITInventory, 'get_all_tables_for_esp', autospec=True)
    @patch.object(ESPInventory, 'get_tables_by_id', autospec=True)
    @patch('ibis.inventory.inventory.Inventory._connect', autospec=True)
    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch.object(VizOozie, 'visualizeXML', autospec=True)
    @patch.object(VizOozie, 'convertDotToPDF', autospec=True)
    @patch.object(PerfInventory, 'insert_freq_ingest', autospec=True)
    def test_gen_prod_workflow_3(self, m_freq_ingest, m_convert_pdf,
                                 m_v_xml, m_eval, m_c,
                                 m_get_id, m_get_t_esp, m_dryrun,
                                 m_put_w, m_sqoop_cache, m_sqoop_cache_view,
                                 m_dryrun_all):
        """ Tests generate_prod_workflows with 6 tables. Two light,
        three medium and one heavy."""
        m_eval.return_value = [['Col1', 'varchar'], ['Col2', 'varchar']]
        m_get_id.side_effect = [appl_ref_id_tbl_01, appl_ref_id_tbl_02]
        _mock_esp_tables_01 = [ItTable(tbl, self.cfg_mgr) for tbl in
                               mock_esp_tables_01]
        m_get_t_esp.return_value = _mock_esp_tables_01
        self.cfg_mgr.env = 'perf'
        status, msg, git_files = self.driver.gen_prod_workflow('FAKED001')
        self.assertEquals(len(git_files), 51)
        self.assertIn('Generated', msg)
        self.assertIn('workflow:', msg)
        self.assertIn('subworkflow:', msg)
        self.assertTrue(status)

    @patch.object(ITInventory, 'get_all_tables_for_esp', autospec=True)
    @patch.object(ESPInventory, 'get_tables_by_id', autospec=True)
    @patch('ibis.inventory.inventory.Inventory._connect', autospec=True)
    def test_gen_prod_workflow_without_applrefs(self, m_c, m_get_tables_by_id,
                                                m_get_all_tables_for_esp):
        """ test_gen_prod_workflow_without_applrefs"""
        m_get_all_tables_for_esp.return_value = [mock_esp_tables_01,
                                                 mock_esp_tables_02]
        m_get_tables_by_id.return_value = []
        status, msg, git_files = self.driver.gen_prod_workflow('FAKED001')
        self.assertEquals(len(git_files), 0)
        self.assertIn(
            "No row found for esp_appl_id: 'FAKED001' "
            "in 'ibis.esp_ids' table", msg)
        self.assertFalse(status)

    @patch.object(ITInventory, 'get_all_tables_for_esp', autospec=True)
    @patch('ibis.inventory.inventory.Inventory._connect', autospec=True)
    def test_gen_prod_workflow_without_tables(self, m_c,
                                              m_get_all_tables_for_esp):
        """ test_gen_prod_workflow_without_applrefs"""
        m_get_all_tables_for_esp.return_value = []
        status, msg, git_files = self.driver.gen_prod_workflow('FAKED001')
        self.assertEquals(len(git_files), 0)
        self.assertIn("No tables found for esp_appl_id: 'FAKED001'", msg)
        self.assertFalse(status)

    @patch.object(Inventory, '_connect', autospec=True)
    def test_export(self, m_con):
        """test export no table"""
        self.driver.it_inventory = MagicMock(spec=ITInventory)
        self.driver.it_inventory.get_table_mapping.return_value = light_3_prop
        result = self.driver.export('fake_database', 'fake_cens_tablename',
                                    'fake_domain.fake_database_fake_cens_tablename')
        print result
        self.assertIn('_export.xml, generated to', result)

    def test_export_no_tbl(self):
        """test export no table"""
        self.driver.it_inventory = MagicMock(spec=ITInventory)
        self.driver.it_inventory.get_table_mapping.return_value = {}
        result = self.driver.export('fake_database', 'fake_fa_tablename',
                                    'member.fake_database'
                                    '_fake_fa_tablename')
        self.assertIn(
            'doesn\'t exist in the it_table export directory '
            'could not be found.',
            result)

    def test_export_fail(self):
        """test export no table"""
        self.driver.it_inventory = MagicMock(spec=ITInventory)
        self.driver.it_inventory.get_table_mapping.return_value = {}
        result = self.driver.export(
            'fake_database', 'fake_fa_tablename',
            'memberfake_database_fake_fa_tablename')
        self.assertIn('Please provide an appropriate --to', result)

    @patch.object(Driver, 'submit_it_file', autospec=True)
    @patch('ibis.driver.driver.create', autospec=True)
    def test_gen_it_table_with_split_by(self, mock_it_table_gen_create,
                                        mock_submit_it_file):
        """Test split by wrapper."""
        with patch('__builtin__.open') as m_open:
            m_open.readlines.return_value = MagicMock(spec=file)
        self.driver.gen_it_table_with_split_by(m_open, 45)

    @patch.object(Inventory, '_connect', autospec=True)
    def test_generate_subworkflow(self, m_con):
        result = self.driver.generate_subworkflow('test_generate_subworkflow',
                                                  ['files'])
        self.assertIn('Generated subworkflow', result)

    def test_group_workflows(self):
        """test _group_workflows"""
        ws_path = self.cfg_mgr.oozie_workspace
        generated_wfs = [
            ws_path + 'file1.xml', ws_path + 'file2.xml',
            ws_path + 'file3.xml', ws_path + 'file4.xml',
            ws_path + 'file5.xml', ws_path + 'file6.xml',
            ws_path + 'file7.xml', ws_path + 'file8.xml',
            ws_path + 'file9.xml', ws_path + 'file10.xml',
            ws_path + 'file11.xml', ws_path + 'file12.xml',
            ws_path + 'file13.xml', ws_path + 'file14.xml',
            ws_path + 'file15.xml', ws_path + 'file16.xml',
            ws_path + 'file17.xml', ws_path + 'file18.xml']

        chunks = self.driver._group_workflows(generated_wfs)
        self.assertEquals(len(chunks), 4)
        self.assertIn(ws_path + 'file18.xml', chunks[3])

    def test_parse_kite_request(self):
        required_fields = ['source_database_name', 'source_table_name',
                           'hdfs_loc']
        optional_fields = []
        expected_request = [{'source_database_name': 'database_one',
                             'hdfs_loc': '/test/hdfs/loc',
                             'source_table_name': 'table_one'},
                            {'source_database_name': 'database_one',
                             'hdfs_loc': '/test/hdfs/loc',
                             'source_table_name': 'table_two'}]
        file_h = open(os.path.join(BASE_DIR,
                                   'test_resources/kite_request.txt'), 'r')
        requests, msg, _ = parse_file_by_sections(file_h, '[Request]',
                                                  required_fields,
                                                  optional_fields)
        self.assertEquals(expected_request, requests)

    @patch.object(Driver, '_get_workflow_name', return_value="kite_request")
    def test_gen_kite_workflow(self, mock1):
        file_h = open(os.path.join(BASE_DIR,
                                   'test_resources/kite_request.txt'), 'r')
        result = self.driver.gen_kite_workflow(file_h)
        self.assertIsNotNone(result)

    def test_gen_schedule_subworkflows_int_sys(self):
        """test gen schedule subworkflows for same table from sys and int"""
        sub_wf_file_name = 'test_subwf'
        tab1 = ItTable(sqls_int, self.cfg_mgr)
        tab2 = ItTable(sqls_sys, self.cfg_mgr)
        heavy_tables = [tab1, tab2]
        workflows_chunks = [[tab1, 'int_full_load'],
                            [tab2, 'sys_full_load']]
        gen_files = self.driver.gen_schedule_subworkflows(
            sub_wf_file_name, workflows_chunks, heavy_tables, 'FAKED001')
        self.assertEquals(len(gen_files), 4)
        expected_file = os.path.join(
            BASE_DIR, 'test_resources/subwf_sys_int.xml')
        test_file = os.path.join(self.cfg_mgr.files, 'test_subwf.xml')
        self.assertTrue(self.files_equal(expected_file, test_file))

    def test_gen_schedule_subworkflows_light(self):
        """test gen schedule subworkflows for 5 light tables"""
        sub_wf_file_name = 'test_subwf'
        tab1 = ItTable(fake_cens_tbl_prop, self.cfg_mgr)
        tab2 = ItTable(light_3_prop, self.cfg_mgr)
        tab3 = ItTable(light_4_prop, self.cfg_mgr)
        tab4 = ItTable(light_5_prop, self.cfg_mgr)
        tab5 = ItTable(fake_ben_tbl_prop, self.cfg_mgr)
        light_tables = [tab1, tab2, tab3, tab4, tab5]
        workflows_chunks = [
            [tab1, 'tab1_full_load'],
            [tab2, 'tab2_full_load'],
            [tab3, 'tab3_full_load'],
            [tab4, 'tab4_full_load'],
            [tab5, 'tab5_full_load']]
        gen_files = self.driver.gen_schedule_subworkflows(
            sub_wf_file_name, workflows_chunks, light_tables, 'FAKED001')
        self.assertEquals(len(gen_files), 4)
        expected_file = os.path.join(
            BASE_DIR, 'test_resources/subwf_light.xml')
        test_file = os.path.join(self.cfg_mgr.files, 'test_subwf.xml')
        self.assertTrue(self.files_equal(expected_file, test_file))

    def test_gen_schedule_subworkflows_heavy(self):
        """test gen schedule subworkflows for 3 heavy tables"""
        sub_wf_file_name = 'test_subwf'
        tab1 = ItTable(heavy_2_prop, self.cfg_mgr)
        tab2 = ItTable(heavy_3_prop, self.cfg_mgr)
        tab3 = ItTable(full_ingest_tbl_mysql, self.cfg_mgr)

        heavy_tables = [tab1, tab2, tab3]
        workflows_chunks = [
            [tab1, 'tab1_full_load'],
            [tab2, 'tab2_full_load'],
            [tab3, 'tab3_full_load']]
        gen_files = self.driver.gen_schedule_subworkflows(
            sub_wf_file_name, workflows_chunks, heavy_tables, 'FAKED001')
        self.assertEquals(len(gen_files), 4)
        expected_file = os.path.join(
            BASE_DIR, 'test_resources/subwf_heavy.xml')
        test_file = os.path.join(self.cfg_mgr.files, 'test_subwf.xml')
        self.assertTrue(self.files_equal(expected_file, test_file))

    def test_gen_schedule_subworkflows_mixed(self):
        """test gen schedule subworkflows for
        5 light tables, 3 medium tables, 3 heavy tables
        """
        sub_wf_file_name = 'test_subwf'
        # heavy
        tab1 = ItTable(heavy_2_prop, self.cfg_mgr)
        tab2 = ItTable(heavy_3_prop, self.cfg_mgr)
        tab3 = ItTable(full_ingest_tbl_mysql, self.cfg_mgr)

        # medium
        tab4 = ItTable(fake_fct_tbl_prop, self.cfg_mgr)
        tab5 = ItTable(fake_fact_tbl_prop, self.cfg_mgr)
        tab6 = ItTable(mock_table_mapping_val, self.cfg_mgr)

        # small
        tab7 = ItTable(fake_cens_tbl_prop, self.cfg_mgr)
        tab8 = ItTable(light_3_prop, self.cfg_mgr)
        tab9 = ItTable(light_4_prop, self.cfg_mgr)
        tab10 = ItTable(light_5_prop, self.cfg_mgr)
        tab11 = ItTable(fake_ben_tbl_prop, self.cfg_mgr)

        all_tables = [tab1, tab2, tab3, tab4, tab5, tab6, tab7,
                      tab8, tab9, tab10, tab11]
        workflows_chunks = [
            [tab1, 'tab1_heavy'],
            [tab2, 'tab2_heavy'],
            [tab3, 'tab3_heavy'],
            [tab4, 'tab4_medium'],
            [tab5, 'tab5_medium'],
            [tab6, 'tab6_medium'],
            [tab7, 'tab7_small'],
            [tab8, 'tab8_small'],
            [tab9, 'tab9_small'],
            [tab10, 'tab10_small'],
            [tab11, 'tab11_small']]
        gen_files = self.driver.gen_schedule_subworkflows(
            sub_wf_file_name, workflows_chunks, all_tables, 'FAKED001')
        self.assertEquals(len(gen_files), 4)
        expected_file = os.path.join(
            BASE_DIR, 'test_resources/subwf_mixed.xml')
        test_file = os.path.join(self.cfg_mgr.files, 'test_subwf.xml')
        self.assertTrue(self.files_equal(expected_file, test_file))

    def test_determine_auto_domain(self):
        tab1 = ItTable(full_ingest_tbl_auto_domain, self.cfg_mgr)
        self.driver.add_env_to_domain(tab1)

    def test_determine_auto_domain_env(self):
        tab1 = ItTable(full_ingest_tbl_auto_domain_env, self.cfg_mgr)
        self.driver.add_env_to_domain(tab1)

    @patch('ibis.driver.driver.RequestInventory.get_available_requests',
           autospec=True)
    @patch('ibis.driver.driver.Driver.gen_prod_workflow',
           autospec=True)
    @patch('ibis.driver.driver.Driver.update_it_table', autospec=True)
    def test_gen_prod_workflow_tables(self, m_s_it_file,
                                      gen_prod_workflow,
                                      mock_get_available_requests):
        mock_get_available_requests.return_value = \
            ([ItTable(mock_table_mapping_val, self.cfg_mgr)], [], [])
        gen_prod_workflow.return_value = (None, None, None)
        file_h = open(
            os.path.join(BASE_DIR, 'test_resources/request_test_valid.txt'),
            'r')
        self.assertTrue(self.driver.gen_prod_workflow_tables(file_h))

    @patch('ibis.driver.driver.subprocess', autospec=True)
    @patch.object(Inventory, 'get_table_mapping',
                  return_value=fake_fct_tbl_prop)
    def test_retrieve_backup(self, mock_get_table_mapping,
                             mock_subprocess):
        """test retrieve_backup"""
        tbl = fake_fct_tbl_prop
        msg = self.driver.retrieve_backup(tbl['source_database_name'],
                                          tbl['source_table_name'])
        self.assertEquals(msg, 'Retrieving backup for fake_database_fake_fct_tablename\n')

    @patch('ibis.inventory.it_inventory.ITInventory.update',
           autospec=True)
    @patch('ibis.driver.driver.RequestInventory.get_available_requests',
           autospec=True)
    @patch('ibis.driver.driver.Driver.gen_prod_workflow',
           autospec=True)
    @patch('ibis.driver.driver.Driver.update_it_table', autospec=True)
    def test_gen_prod_workflow_tables_noapp(self, m_s_it_file,
                                            gen_prod_workflow,
                                            mock_get_available_requests,
                                            mock_update):
        mock_get_available_requests.return_value = \
            ([ItTable(mock_table_mapping_val_app, self.cfg_mgr)], [], [])
        gen_prod_workflow.return_value = (None, None, None)
        file_h = open(
            os.path.join(BASE_DIR, 'test_resources/request_test_valid.txt'),
            'r')
        mock_update.return_value = (True, 'Update Success')
        self.assertTrue(self.driver.gen_prod_workflow_tables(file_h))

    @patch('ibis.driver.driver.Driver.update_it_table_export')
    @patch('ibis.driver.driver.RequestInventory.get_available_requests_export',
           autospec=True)
    def test_export_request(self, mock_get_available_requests,
                            mock_upate):
        mock_get_available_requests.return_value = \
            ([ItTableExport(heavy_3_prop_exp, self.cfg_mgr)], [])
        request_file = open(
            os.path.join(BASE_DIR, 'test_resources/export_request.txt'),
            'r')
        status, _ = self.driver.export_request(request_file, False)
        self.assertTrue(status)

    def test_export_oracle(self):
        self.driver.export_oracle("source_table_name",
                                  "source_database_name",
                                  "source_dir", "jdbc_url",
                                  "update_key",
                                  "target_table_name",
                                  "target_database_name",
                                  "user_name", "password_alias")

    def test_export_teradata(self):
        self.driver.export_teradata("source_table_name",
                                    "source_database_name",
                                    "source_dir", "jdbc_url",
                                    "target_table_name",
                                    "target_database_name",
                                    "user_name", "password_alias")

    @patch('subprocess.call')
    def test_retrieve_backup_notarget(self, mock_call):
        """
        Given arguments for retrieve_back up
        expects print statement
        """
        self.driver.it_inventory.get_table_mapping = MagicMock(
            spec=ITInventory.get_table_mapping)
        self.driver.it_inventory.get_table_mapping.return_value = \
            mock_table_mapping_val
        self.driver.utilities.run_subprocess = \
            MagicMock(spec=Utilities.run_subprocess)
        self.driver.utilities.run_subprocess.side_effect = [0, 1, 1, 1, 1, 1]
        msg = self.driver.retrieve_backup("db_name", "table_name")
        self.assertEqual(msg, "Retrieving backup for " +
                         "db_name_table_name\nTarget directory doesn't " +
                         "exist in it_table.\n")

    @patch('subprocess.call')
    def test_retrieve_backup_iftarget(self, mock_popen):
        """
        Given arguments for retrieve_back up
        expects print statement
        """
        self.driver.it_inventory.get_table_mapping = MagicMock(
            spec=ITInventory.get_table_mapping)
        self.driver.it_inventory.get_table_mapping.return_value = \
            heavy_3_prop_exp
        self.driver.utilities.run_subprocess = \
            MagicMock(spec=Utilities.run_subprocess)
        self.driver.utilities.run_subprocess.side_effect = [0, 1, 1, 1, 1, 1]
        msg = self.driver.retrieve_backup("db_name", "table_name")
        self.assertEqual(msg, "Retrieving backup for db_name_table_name\n" +
                         "Failed to copy parquet_live.hql\nFailed to copy " +
                         "avro_parquet.hql\nFailed to copy files to live.\n")

    @patch('subprocess.call')
    def test_retrieve_backup_iftable(self, mock_popen):
        """
        Given arguments for retrieve_back up
        expects print statement
        """
        self.driver.it_inventory.get_table_mapping = MagicMock(
            spec=ITInventory.get_table_mapping)
        self.driver.it_inventory.get_table_mapping.return_value = {}
        msg = self.driver.retrieve_backup("db_name", "table_name")
        self.assertEqual(msg, "Retrieving backup for db_name_table_name\n" +
                         "db_name_{} does not exist in it_table. Directory " +
                         "can't be found\n")

    @patch('getpass.getuser')
    def test_get_config_workflow_name(self, m_getuser):
        """test _get_config_workflow_name"""
        m_getuser.return_value = 'userId'
        mock_tmp = MagicMock()
        mock_tmp.name = '/path/to/requestFile.txt'
        val = self.driver._get_config_workflow_name(mock_tmp)
        self.assertEquals(val, 'userId_config_wf_requestFile')

    @patch('getpass.getuser')
    def test_get_workflow_name(self, m_getuser):
        """test _get_workflow_name"""
        m_getuser.return_value = 'userId'
        mock_tmp = MagicMock()
        mock_tmp.name = '/path/to/requestFile.txt'
        val = self.driver._get_workflow_name(mock_tmp)
        self.assertEquals(val, 'dev_userId_requestFile')

    @patch('getpass.getuser')
    def test_get_subworkflow_name_prefix(self, m_getuser):
        """test _get_subworkflow_name_prefix"""
        m_getuser.return_value = 'userId'
        mock_tmp = MagicMock()
        mock_tmp.name = '/path/to/requestFile.txt'
        val = self.driver._get_subworkflow_name_prefix(mock_tmp)
        self.assertEquals(val, 'userId_requestFile')

    @patch('getpass.getuser')
    def test_get_workflow_name_table(self, m_getuser):
        """test _get_workflow_name_table"""
        m_getuser.return_value = 'userId'
        tab1 = ItTable(fake_cens_tbl_prop, self.cfg_mgr)
        val = self.driver._get_workflow_name_table(tab1)
        self.assertEquals(val, 'dev_userId_fake_database_fake_cens_tablename')

    @patch('getpass.getuser')
    def test_get_workflow_name_table_export(self, m_getuser):
        """test _get_workflow_name_table_export"""
        m_getuser.return_value = 'userId'
        tab1 = ItTable(fake_cens_tbl_prop, self.cfg_mgr)
        val = self.driver._get_workflow_name_table_export(tab1)
        self.assertEquals(val, 'dev_userId_fake_database_fake_cens_tablename_export')

    def test_get_prod_table_workflow_name(self):
        """test _get_prod_table_workflow_name"""
        tab1 = ItTable(fake_cens_tbl_prop, self.cfg_mgr)
        val = self.driver._get_prod_table_workflow_name(tab1)
        self.assertEquals(val, 'fake_database_fake_cens_tablename')

    @patch('getpass.getuser')
    def test_get_incr_workflow_name(self, m_getuser):
        """test _get_incr_workflow_name"""
        m_getuser.return_value = 'userId'
        tab1 = ItTable(fake_cens_tbl_prop, self.cfg_mgr)
        val = self.driver._get_incr_workflow_name(tab1)
        self.assertEquals(val, 'dev_userId_incr_fake_cens_tablename')

    @patch.object(PerfInventory, 'insert_freq_ingest')
    def test_insert_freq_ingest_driver(self, m_freq_ingest):
        """ test freq_ingest_driver"""
        self.driver.insert_freq_ingest_driver(['mock_team_nm'],
                                              ['daily'],
                                              ['mock_table_nm'],
                                              ['no'])
        m_freq_ingest.assert_called_once_with(['mock_team_nm'],
                                              ['daily'],
                                              ['mock_table_nm'],
                                              ['no'])

    @patch.object(PerfInventory, 'insert_freq_ingest')
    def test_insert_freq_ingest_driver_fa(self, m_freq_ingest):
        """ test freq_ingest_driver for frequency and actovor are none"""
        with self.assertRaises(ValueError) as context:
            self.driver.insert_freq_ingest_driver(['mock_team_nm'],
                                                  None,
                                                  ['mock_table_nm'],
                                                  None)
        error = "Either of frequency or activate column must contain value"
        self.assertTrue(error in str(context.exception))

    @patch.object(PerfInventory, 'wipe_perf_env')
    def test_wipe_perf_env_driver(self, m_wipe_perf):
        """test wipe_perf_driver with team name"""
        self.driver.wipe_perf_env_driver(['fake_view_im'], False)
        m_wipe_perf.assert_called_once_with('fake_view_im', False)

    @patch.object(PerfInventory, 'wipe_perf_env', autospec=True)
    def test_wipe_perf_env_driver_ibis(self, m_wipe_perf):
        """test wipe_perf_driver with domain as ibis"""
        with self.assertRaises(ValueError) as context:
            self.driver.wipe_perf_env_driver(['ibis'], False)
        self.assertTrue('Cannot wipe Ibis database' in
                        str(context.exception))

    @patch.object(PerfInventory, 'wipe_perf_env', autospec=True)
    def test_wipe_perf_env_driver_domain(self, m_wipe_perf):
        """test wipe_perf_driver with team name as domain"""
        with self.assertRaises(ValueError) as context:
            self.driver.wipe_perf_env_driver(['domain1'], False)
        self.assertTrue('Team name provided is Domain, please \
             provide your team name' in str(context.exception))

if __name__ == "__main__":
    unittest.main()
