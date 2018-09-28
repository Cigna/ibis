"""Workflow generator tests."""
import collections
import difflib
import os
import shutil
import unittest
from mock import patch
from ibis.inventor.tests.fixture_workflow_generator import *
from ibis.inventor.workflow_generator import WorkflowGenerator
from ibis.inventory import inventory
from ibis.inventory.request_inventory import RequestInventory, Request
from ibis.model.table import ItTable
from ibis.settings import UNIT_TEST_ENV
from ibis.utilities.config_manager import ConfigManager
from ibis.driver.driver import Driver
from ibis.model.exporttable import ItTableExport

# Properties
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class WorkflowGeneratorFunctionsTest(unittest.TestCase):
    """Tests the functionality of the Workflow Generator class..."""

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    @patch.object(inventory.Inventory, '_connect', autospec=True)
    def setUp(self, mock_connect):
        self.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        self.req_inventory = RequestInventory(self.cfg_mgr)
        self.driver = Driver(self.cfg_mgr)
        self.generator = WorkflowGenerator('test_workflow', self.cfg_mgr)
        # Expected workflows hardcoded
        self.generator.action_builder.cfg_mgr.host = \
            'fake.workflow.host'
        # Hadoop credstore enabled case
        self.cfg_mgr.hadoop_credstore_password_disable = False
        self.generator.action_builder.workflowName = 'test_workflow'

    def tearDown(self):
        self.generator.file_out.close()
        self.generator = None

    def files_equal(self, test_file, expected_file):
        """Compares two files"""
        same = True
        test_fh = open(test_file, 'r')
        fo_gen = open(expected_file, 'r')
        test_str = test_fh.read()
        expected_str = fo_gen.read()

        if not self.strings_equal(test_str, expected_str):
            same = False
            print "Generated test file:{0}".format(test_file)
            print "Fix the file:{0}".format(expected_file)

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
            print "\nFiles don't match... "
            diff = difflib.unified_diff(expected_str, test_str)
            print '\n'.join(list(diff))
        return same

    def test_sort_table_prop_by_load(self):
        """Tests that a map of tables gets sorted by load """
        tables = [ItTable(fake_fct_tbl_prop, self.cfg_mgr),
                  ItTable(fake_prof_tbl_prop, self.cfg_mgr),
                  ItTable(fake_fact_tbl_prop, self.cfg_mgr),
                  ItTable(fake_ben_tbl_prop, self.cfg_mgr),
                  ItTable(fake_cens_tbl_prop, self.cfg_mgr)]
        expected = {'100': [ItTable(fake_ben_tbl_prop, self.cfg_mgr),
                            ItTable(fake_cens_tbl_prop, self.cfg_mgr)],
                    '010': [ItTable(fake_fct_tbl_prop, self.cfg_mgr),
                            ItTable(fake_fact_tbl_prop, self.cfg_mgr)],
                    '001': [ItTable(fake_prof_tbl_prop, self.cfg_mgr)]}
        expected = collections.OrderedDict(sorted(expected.items()))
        result = self.generator.sort_table_prop_by_load(tables)
        equals = True
        for key, val in expected.iteritems():
            if key in result:
                for i, table in enumerate(val):
                    if not result[key][i] == table:
                        equals = False
            else:
                equals = False
        self.assertTrue(equals)

    def compare_pipelines(self, pipelines1, pipeline2):
        """Compare two pipelines"""
        equals = True
        for i, pipeline in enumerate(pipelines1):
            oth_pipeline = pipeline2[i]
            for j, table in enumerate(pipeline):
                if not oth_pipeline[j] == table:
                    equals = False
        return equals

    def test_gen_pipelines(self):
        """Tests the pipeline list generation"""
        # One light table
        tbl_prop = [ItTable(fake_ben_tbl_prop, self.cfg_mgr)]
        sorted_load = self.generator.sort_table_prop_by_load(tbl_prop)
        bool_test = self.compare_pipelines(
            self.generator.gen_pipelines(sorted_load),
            [[ItTable(fake_ben_tbl_prop, self.cfg_mgr)]])
        self.assertTrue(bool_test)
        # 4 Light tables
        tbl_prop = [ItTable(fake_ben_tbl_prop, self.cfg_mgr),
                    ItTable(fake_ben_tbl_prop, self.cfg_mgr),
                    ItTable(fake_cens_tbl_prop, self.cfg_mgr),
                    ItTable(fake_cens_tbl_prop, self.cfg_mgr)]
        sorted_load = self.generator.sort_table_prop_by_load(tbl_prop)
        bool_test = self.compare_pipelines(
            self.generator.gen_pipelines(sorted_load),
            [[ItTable(fake_ben_tbl_prop, self.cfg_mgr),
              ItTable(fake_ben_tbl_prop, self.cfg_mgr),
              ItTable(fake_cens_tbl_prop, self.cfg_mgr),
              ItTable(fake_cens_tbl_prop, self.cfg_mgr)]])
        self.assertTrue(bool_test)

        # Odd light tables
        tbl_prop = [ItTable(fake_ben_tbl_prop, self.cfg_mgr),
                    ItTable(fake_ben_tbl_prop, self.cfg_mgr),
                    ItTable(fake_ben_tbl_prop, self.cfg_mgr),
                    ItTable(fake_cens_tbl_prop, self.cfg_mgr),
                    ItTable(fake_cens_tbl_prop, self.cfg_mgr),
                    ItTable(fake_cens_tbl_prop, self.cfg_mgr)]
        sorted_load = self.generator.sort_table_prop_by_load(tbl_prop)
        bool_test = self.compare_pipelines(
            self.generator.gen_pipelines(sorted_load),
            [[ItTable(fake_ben_tbl_prop, self.cfg_mgr),
              ItTable(fake_ben_tbl_prop, self.cfg_mgr),
              ItTable(fake_ben_tbl_prop, self.cfg_mgr),
              ItTable(fake_cens_tbl_prop, self.cfg_mgr)],
             [ItTable(fake_cens_tbl_prop, self.cfg_mgr),
              ItTable(fake_cens_tbl_prop, self.cfg_mgr)]])
        self.assertTrue(bool_test)

        # 2 medium
        tbl_prop = [ItTable(fake_fct_tbl_prop, self.cfg_mgr),
                    ItTable(fake_fact_tbl_prop, self.cfg_mgr)]
        sorted_load = self.generator.sort_table_prop_by_load(tbl_prop)
        bool_test = self.compare_pipelines(
            self.generator.gen_pipelines(sorted_load),
            [[ItTable(fake_fct_tbl_prop, self.cfg_mgr),
              ItTable(fake_fact_tbl_prop, self.cfg_mgr)]])
        self.assertTrue(bool_test)

        # Odd medium
        tbl_prop = [ItTable(fake_fct_tbl_prop, self.cfg_mgr),
                    ItTable(fake_fact_tbl_prop, self.cfg_mgr),
                    ItTable(fake_fact_tbl_prop, self.cfg_mgr)]
        sorted_load = self.generator.sort_table_prop_by_load(tbl_prop)
        bool_test = self.compare_pipelines(
            self.generator.gen_pipelines(sorted_load),
            [[ItTable(fake_fct_tbl_prop, self.cfg_mgr),
              ItTable(fake_fact_tbl_prop, self.cfg_mgr)],
             [ItTable(fake_fact_tbl_prop, self.cfg_mgr)]])
        self.assertTrue(bool_test)

        # 2 heavy
        tbl_prop = [ItTable(fake_prof_tbl_prop, self.cfg_mgr),
                    ItTable(fake_prof_tbl_prop, self.cfg_mgr)]
        sorted_load = self.generator.sort_table_prop_by_load(tbl_prop)
        bool_test = self.compare_pipelines(
            self.generator.gen_pipelines(sorted_load),
            [[ItTable(fake_prof_tbl_prop, self.cfg_mgr),
              ItTable(fake_prof_tbl_prop, self.cfg_mgr)]])
        self.assertTrue(bool_test)

        # odd heavy
        tbl_prop = [ItTable(fake_prof_tbl_prop, self.cfg_mgr),
                    ItTable(fake_prof_tbl_prop, self.cfg_mgr),
                    ItTable(fake_prof_tbl_prop, self.cfg_mgr)]
        sorted_load = self.generator.sort_table_prop_by_load(tbl_prop)
        self.assertEqual(
            self.generator.gen_pipelines(sorted_load),
            [[ItTable(fake_prof_tbl_prop, self.cfg_mgr),
              ItTable(fake_prof_tbl_prop, self.cfg_mgr)],
             [ItTable(fake_prof_tbl_prop, self.cfg_mgr)]])

        # 5 light, 3 medium ,3 heavy
        tbl_prop = [ItTable(fake_ben_tbl_prop, self.cfg_mgr),
                    ItTable(fake_ben_tbl_prop, self.cfg_mgr),
                    ItTable(fake_ben_tbl_prop, self.cfg_mgr),
                    ItTable(fake_cens_tbl_prop, self.cfg_mgr),
                    ItTable(fake_cens_tbl_prop, self.cfg_mgr),
                    ItTable(fake_fct_tbl_prop, self.cfg_mgr),
                    ItTable(fake_fact_tbl_prop, self.cfg_mgr),
                    ItTable(fake_fact_tbl_prop, self.cfg_mgr),
                    ItTable(fake_prof_tbl_prop, self.cfg_mgr),
                    ItTable(fake_prof_tbl_prop, self.cfg_mgr),
                    ItTable(fake_prof_tbl_prop, self.cfg_mgr)]
        sorted_load = self.generator.sort_table_prop_by_load(tbl_prop)
        self.assertEqual(self.generator.gen_pipelines(sorted_load),
                         [[ItTable(fake_ben_tbl_prop, self.cfg_mgr),
                           ItTable(fake_ben_tbl_prop, self.cfg_mgr),
                           ItTable(fake_ben_tbl_prop, self.cfg_mgr),
                           ItTable(fake_cens_tbl_prop, self.cfg_mgr)],
                          [ItTable(fake_fct_tbl_prop, self.cfg_mgr),
                           ItTable(fake_fact_tbl_prop, self.cfg_mgr)],
                          [ItTable(fake_prof_tbl_prop, self.cfg_mgr),
                           ItTable(fake_prof_tbl_prop, self.cfg_mgr)],
                          [ItTable(fake_cens_tbl_prop, self.cfg_mgr),
                           ItTable(fake_fact_tbl_prop, self.cfg_mgr)],
                          [ItTable(fake_prof_tbl_prop, self.cfg_mgr)]])

    def test_get_pipeline_weight(self):
        """Tests the method for checking a pipeline's weight"""
        light_pipeline = [ItTable(fake_ben_tbl_prop, self.cfg_mgr)]
        self.assertEqual((True, False, False),
                         self.generator.get_pipeline_weight(light_pipeline))
        med_pipeline = [ItTable(fake_fact_tbl_prop, self.cfg_mgr)]
        self.assertEqual((False, True, False),
                         self.generator.get_pipeline_weight(med_pipeline))
        heavy_pipeline = [ItTable(fake_prof_tbl_prop, self.cfg_mgr)]
        self.assertEqual((False, False, True),
                         self.generator.get_pipeline_weight(heavy_pipeline))
        mixed_pipeline = [ItTable(fake_ben_tbl_prop, self.cfg_mgr),
                          ItTable(fake_fact_tbl_prop, self.cfg_mgr),
                          ItTable(fake_prof_tbl_prop, self.cfg_mgr)]
        self.assertEqual((True, True, True),
                         self.generator.get_pipeline_weight(mixed_pipeline))

    @patch.object(ItTable, 'load', return_value='000')
    def test_get_pipeline_weight_fail(self, mock_get_load):
        """Tests the method for checking a pipeline's weight"""
        light_pipeline = [ItTable(fake_ben_tbl_prop, self.cfg_mgr)]
        self.assertEqual((False, False, False),
                         self.generator.get_pipeline_weight(light_pipeline))

    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    def test_gen_full_ingest_actions(self, m_eval):
        """Tests workflow actions generation for full ingest"""
        m_eval.return_value = [['Col1', 'TIMESTAMP'], ['Col2', 'TIMESTAMP'],
                               ['Col3', 'varchar']]
        table = ItTable(fake_ben_tbl_prop, self.cfg_mgr)
        self.generator.gen_full_ingest_actions(table, {
            'sqoop_to': 'fake_ben_tablename_avro',
            'end_to': 'end'})
        self.generator.file_out.close()
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow.xml'),
            os.path.join(BASE_DIR, 'expected_workflows/workflow_action.xml'))
        self.assertTrue(bool_test)

    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    def test_gen_full_ingest_actions_pwdfile(self, m_eval):
        """Tests workflow actions generation for full ingest
        (Hadoop credstr disabled)"""
        m_eval.return_value = [['Col1', 'TIMESTAMP'], ['Col2', 'TIMESTAMP'],
                               ['Col3', 'varchar']]
        self.generator.gen_full_ingest_actions(
            ItTable(fake_ben_pwdfile_tbl_prop, self.cfg_mgr),
            {'sqoop_to': 'fake_ben_tablename_avro', 'end_to': 'end'})
        self.generator.file_out.close()
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow.xml'),
            os.path.join(BASE_DIR,
                         'expected_workflows/workflow_action_pf.xml'))
        self.assertTrue(bool_test)

    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch.object(inventory.Inventory, '_connect', autospec=True)
    def test_gen_full_ingest_actions_pwdfile_dev(self, mock_connect, m_eval):
        """Tests workflow actions generation for full ingest
         (Hadoop credstr enabled)"""
        col_type_eval = [['Col1', 'TIMESTAMP'], ['Col2', 'TIMESTAMP'],
                         ['Col3', 'varchar']]
        m_eval.side_effect = (col_type_eval, [['table']])
        self.generator = WorkflowGenerator('test_workflow_dev', self.cfg_mgr)
        self.cfg_mgr.hadoop_credstore_password_disable = True
        self.generator.gen_full_ingest_actions(
            ItTable(fake_ben_tbl_prop, self.cfg_mgr),
            {'sqoop_to': 'fake_ben_tablename_avro', 'end_to': 'end'})
        self.generator.file_out.close()
        self.assertTrue(self.cfg_mgr.hadoop_credstore_password_disable,
                        'Hives cresdtore is disabled')
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow_dev.xml'),
            os.path.join(BASE_DIR,
                         'expected_workflows/workflow_action_pf_dev.xml'))
        self.assertTrue(bool_test)

    def test_gen_full_ingest_actions_authinfo_fail(self):
        """test if auth info is missing"""
        self.generator = WorkflowGenerator('test_workflow_dev', self.cfg_mgr)
        ok_to_action = {'sqoop_to': 'test', 'end_to': 'test'}
        it_table = ItTable(fake_ben_pwdfile_tbl_prop, self.cfg_mgr)
        # empty username value
        it_table.username = ''
        with self.assertRaises(ValueError) as exp_cm:
            self.generator.gen_full_ingest_actions(it_table, ok_to_action)
        test_err_msg = open(
            os.path.join(BASE_DIR, 'expected/auth_info_missing.txt')).read()
        bool_test = self.strings_equal(
            exp_cm.exception.message, test_err_msg)
        self.assertTrue(bool_test)

    def test_gen_oracle_export_pwdfile(self):
        """Tests workflow actions generation for Oracle export"""
        self.generator.gen_oracle_export(
            'fake_database_fake_rule_tablename', 'member',
            '/user/data/mdm/member/fake_database/fake_rule_tablename',
            'jdbc:oracle:thin:@//fake.oracle:'
            '1600/fake_servicename',
            'RULE_KEY', 'fake_rule_tablename', 'FAKE_SCHEMA', 'fake_username',
            'fake.password.alias')
        self.generator.file_out.close()
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow.xml'),
            os.path.join(BASE_DIR,
                         'expected_workflows/export_oracle_pf.xml'))
        self.assertTrue(bool_test)

    def test_gen_oracle_export_pwdalias(self):
        """Tests workflow actions generation with jceks for Oracle export"""
        self.generator.gen_oracle_export(
            'fake_database_fake_rule_tablename', 'member',
            '/user/data/mdm/member/fake_database/fake_rule_tablename',
            'jdbc:oracle:thin:@//fake.oracle:'
            '1600/fake_servicename',
            'RULE_KEY', 'fake_rule_tablename', 'FAKE_SCHEMA', 'fake_username',
            'jceks://hdfs/user/dev/fake.passwords.jceks#fake.password.alias')
        self.generator.file_out.close()
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow.xml'),
            os.path.join(BASE_DIR,
                         'expected_workflows/export_oracle.xml'))
        self.assertTrue(bool_test)

    @patch.object(inventory.Inventory, '_connect', autospec=True)
    def test_gen_oracle_export_pwdalias_dev_jcek(self, mock_connect):
        """Tests workflow actions generation with jceks for Oracle export"""
        self.cfg_mgr.hadoop_credstore_password_disable = True
        self.generator = WorkflowGenerator('test_workflow_dev_jceks',
                                           self.cfg_mgr)
        self.generator.gen_oracle_export(
            'fake_database_fake_rule_tablename', 'member',
            '/user/data/mdm/member/fake_database/fake_rule_tablename_clone',
            'jdbc:oracle:thin:@//fake.oracle:'
            '1600/fake_servicename',
            'RULE_KEY', 'fake_rule_tablename', 'FAKE_SCHEMA', 'fake_username',
            'jceks://hdfs/user/dev/fake.passwords.jceks#fake.password.alias')
        self.generator.file_out.close()
        self.assertTrue(self.cfg_mgr.hadoop_credstore_password_disable,
                        'Hives credstore is disabled')
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow_dev_jceks.xml'),
            os.path.join(BASE_DIR,
                         'expected_workflows/export_oracle_dev.xml'))
        self.assertTrue(bool_test)

    @patch.object(Driver, 'update_it_table_export', autospec=True)
    @patch.object(RequestInventory, 'get_available_requests_export',
                  autospec=True)
    def test_gen_database_export_pwdfile(self, mock_connect, m2):
        """Tests workflow actions generation for Oracle export"""
        tables = []
        tables.append(ItTableExport(heavy_3_prop_exp, self.cfg_mgr))
        tables.append(ItTableExport(heavy_3_prop_exp, self.cfg_mgr))
        mock_connect.return_value = [tables, "", ""]
        path = os.path.join(BASE_DIR, 'test_resources/export_request_pf.txt')
        request_file = open(path)
        requests, msg = self.req_inventory.parse_file_export(request_file)
        tables, _, _ = \
            self.req_inventory.get_available_requests_export(requests)
        self.generator.gen_database_export(tables)
        self.generator.file_out.close()
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow.xml'),
            os.path.join(BASE_DIR,
                         'expected_workflows/export_oracle_pf_db.xml'))
        self.assertTrue(bool_test)

    @patch.object(Driver, 'update_it_table_export', autospec=True)
    @patch.object(RequestInventory, 'get_available_requests_export',
                  autospec=True)
    def test_gen_database_export_pwdalias(self, mock_connect, m2):
        """Tests workflow actions generation with jceks for Oracle export"""
        tables = []
        tables.append(ItTableExport(heavy_3_prop_exp, self.cfg_mgr))
        tables.append(ItTableExport(heavy_3_prop_exp, self.cfg_mgr))
        mock_connect.return_value = [tables, "", ""]
        path = os.path.join(BASE_DIR, 'test_resources/export_request_pa.txt')
        request_file = open(path)
        requests, msg = self.req_inventory.parse_file_export(request_file)
        tables, _, _ = \
            self.req_inventory.get_available_requests_export(requests)
        self.generator.gen_database_export(tables)
        self.generator.file_out.close()
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow.xml'),
            os.path.join(BASE_DIR,
                         'expected_workflows/export_oracle_db.xml'))
        self.assertTrue(bool_test)

    @patch.object(Driver, 'update_it_table_export', autospec=True)
    @patch.object(RequestInventory, 'get_available_requests_export',
                  autospec=True)
    def test_gen_database_export_pwdalias_dev_jcek(self, mock_connect, m2):
        """Tests workflow actions generation with jceks for Oracle export"""
        tables = []
        tables.append(ItTableExport(heavy_3_prop_exp, self.cfg_mgr))
        tables.append(ItTableExport(heavy_3_prop_exp, self.cfg_mgr))
        mock_connect.return_value = [tables, "", ""]
        self.cfg_mgr.hadoop_credstore_password_disable = True
        self.generator = WorkflowGenerator('test_workflow_dev_jceks',
                                           self.cfg_mgr)
        path = os.path.join(BASE_DIR, 'test_resources/export_request_pa.txt')
        request_file = open(path)
        requests, msg = self.req_inventory.parse_file_export(request_file)
        tables, _, _ = \
            self.req_inventory.get_available_requests_export(requests)
        self.generator.gen_database_export(tables)
        self.generator.file_out.close()
        self.assertTrue(self.cfg_mgr.hadoop_credstore_password_disable,
                        'Hives credstore is disabled')
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow_dev_jceks.xml'),
            os.path.join(BASE_DIR,
                         'expected_workflows/export_oracle_dev_db.xml'))
        self.assertTrue(bool_test)

    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch.object(inventory.Inventory, 'get_table_mapping', autospec=True)
    @patch.object(inventory.Inventory, '_connect', autospec=True)
    def test_generate_one_req(self, mock_connect, mock_get_table_mapping,
                              m_eval):
        """Tests the full workflow generation for one table request"""
        m_eval.return_value = [['Col1', 'TIMESTAMP'], ['Col2', 'TIMESTAMP'],
                               ['Col3', 'varchar']]
        mock_get_table_mapping.side_effect = [fake_cens_tbl_prop,
                                              fake_cens_tbl_prop]
        all_req = [Request(light_req_1, self.cfg_mgr)]
        available_tables, _, _ = self.req_inventory.get_available_requests(
            all_req)
        self.generator.generate(available_tables)
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow.xml'),
            os.path.join(BASE_DIR, 'expected_workflows/one_req.xml'))
        self.assertTrue(bool_test)

    @patch('ibis.inventor.action_builder.ActionBuilder.get_col_types',
           autospec=True)
    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch.object(inventory.Inventory, 'get_table_mapping', autospec=True)
    @patch.object(inventory.Inventory, '_connect', autospec=True)
    def test_generate_one_req_custom_scripts(
            self, mock_connect, mock_get_table_mapping, mock_eval,
            m_get_col_types):
        """Tests the full workflow generation for one table request"""
        m_get_col_types.return_value = [('trans_time', 'TIMESTAMP')]
        full_ingest_tbl_custom_config['actions'] = \
            'custom_config_no_views_2.dsl'
        mock_get_table_mapping.side_effect = [full_ingest_tbl_custom_config,
                                              full_ingest_tbl_custom_config]
        all_req = [Request(full_ingest_tbl_custom_config_req, self.cfg_mgr)]
        # setup alternative requests dir
        self.cfg_mgr.requests_dir = os.path.join(
            self.cfg_mgr.files, 'requests')
        os.makedirs(self.cfg_mgr.requests_dir)
        os.makedirs(os.path.join(self.cfg_mgr.requests_dir, 'DEV'))
        self.generator.action_builder.dsl_parser.scripts_dir = \
            self.cfg_mgr.requests_dir
        fixture_config_path = os.path.join(
            BASE_DIR, 'test_resources/custom_config_no_views_2.dsl')
        shutil.copy(fixture_config_path, self.cfg_mgr.requests_dir)
        fixture_hql_path = os.path.join(
            BASE_DIR, 'test_resources/hive_test.hql')
        shutil.copy(fixture_hql_path,
                    os.path.join(self.cfg_mgr.requests_dir, 'DEV'))
        fixture_sh_path = os.path.join(
            BASE_DIR, 'test_resources/shell_test.sh')
        shutil.copy(fixture_sh_path, self.cfg_mgr.requests_dir)
        available_tables, _, _ = self.req_inventory.get_available_requests(
            all_req)
        self.generator.generate(available_tables)
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow.xml'),
            os.path.join(BASE_DIR,
                         'expected_workflows/one_req_custom_scripts.xml'))
        self.assertTrue(bool_test)

    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch.object(inventory.Inventory, 'get_table_mapping', autospec=True)
    @patch.object(inventory.Inventory, '_connect', autospec=True)
    def test_generate_light_groupings_even(self, mock_connect,
                                           mock_get_table_mapping, m_eval):
        """Tests the full workflow generation for grouping light tables even
        into four tables and run concurrent ingestion"""
        m_eval.return_value = [['Col1', 'TIMESTAMP'], ['Col2', 'TIMESTAMP'],
                               ['Col3', 'varchar']]
        mock_get_table_mapping.side_effect = [
            fake_cens_tbl_prop, fake_ben_tbl_prop, light_3_prop, light_4_prop,
            fake_cens_tbl_prop, fake_ben_tbl_prop, light_3_prop, light_4_prop]
        all_req = [Request(light_req_1, self.cfg_mgr),
                   Request(light_req_2, self.cfg_mgr),
                   Request(light_req_3, self.cfg_mgr),
                   Request(light_req_4, self.cfg_mgr)]
        available_tables, _, _ = self.req_inventory.get_available_requests(
            all_req)
        self.generator.generate(available_tables)
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow.xml'),
            os.path.join(BASE_DIR,
                         'expected_workflows/light_grouping_even.xml'))
        self.assertTrue(bool_test)

    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch.object(inventory.Inventory, 'get_table_mapping', autospec=True)
    @patch.object(inventory.Inventory, '_connect', autospec=True)
    def test_generate_light_groupings_odd(self, mock_connect,
                                          mock_get_table_mapping, m_eval):
        """Tests the full workflow generation for grouping light tables odd
        into four tables and run concurrent ingestion"""
        m_eval.return_value = [['Col1', 'TIMESTAMP'], ['Col2', 'TIMESTAMP'],
                               ['Col3', 'varchar']]
        mock_get_table_mapping.side_effect = [
            fake_cens_tbl_prop, fake_ben_tbl_prop, light_3_prop, light_4_prop,
            light_5_prop, fake_cens_tbl_prop, fake_ben_tbl_prop, light_3_prop,
            light_4_prop, light_5_prop]
        all_req = [Request(light_req_1, self.cfg_mgr),
                   Request(light_req_2, self.cfg_mgr),
                   Request(light_req_3, self.cfg_mgr),
                   Request(light_req_4, self.cfg_mgr),
                   Request(light_req_5, self.cfg_mgr)]
        available_tables, _, _ = self.req_inventory.get_available_requests(
            all_req)
        self.generator.generate(available_tables)
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow.xml'),
            os.path.join(BASE_DIR,
                         'expected_workflows/light_grouping_odd.xml'))
        self.assertTrue(bool_test)

    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch.object(inventory.Inventory, 'get_table_mapping', autospec=True)
    @patch.object(inventory.Inventory, '_connect', autospec=True)
    def test_generate_medium_groupings(self, mock_connect,
                                       mock_get_table_mapping, m_eval):
        """Tests the full workflow generation for grouping medium
        tables into two tables
        and run concurrent ingestion"""
        m_eval.return_value = [['Col1', 'TIMESTAMP'], ['Col2', 'TIMESTAMP'],
                               ['Col3', 'varchar']]
        mock_get_table_mapping.side_effect = [
            fake_fact_tbl_prop, fake_fct_tbl_prop, med_3_prop,
            fake_fact_tbl_prop, fake_fct_tbl_prop, med_3_prop]
        all_req = [Request(med_req_1, self.cfg_mgr),
                   Request(med_req_2, self.cfg_mgr),
                   Request(med_req_3, self.cfg_mgr)]
        available_tables, _, _ = self.req_inventory.get_available_requests(
            all_req)
        self.generator.generate(available_tables)
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow.xml'),
            os.path.join(BASE_DIR, 'expected_workflows/medium_grouping.xml'))
        self.assertTrue(bool_test)

    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch.object(inventory.Inventory, 'get_table_mapping', autospec=True)
    @patch.object(inventory.Inventory, '_connect', autospec=True)
    def test_generate_heavy_groupings(self, mock_connect,
                                      mock_get_table_mapping, m_eval):
        """Tests the full workflow generation for grouping heavy tables
        into two tables and run staggered ingestion"""
        m_eval.return_value = [['Col1', 'TIMESTAMP'], ['Col2', 'TIMESTAMP'],
                               ['Col3', 'varchar']]
        # get_table_mapping is called twice
        mock_get_table_mapping.side_effect = [
            fake_prof_tbl_prop, heavy_2_prop, heavy_3_prop, fake_prof_tbl_prop,
            heavy_2_prop, heavy_3_prop]
        all_req = [Request(heavy_req_1, self.cfg_mgr),
                   Request(heavy_req_2, self.cfg_mgr),
                   Request(heavy_req_3, self.cfg_mgr)]
        available_tables, _, _ = self.req_inventory.get_available_requests(
            all_req)
        self.generator.generate(available_tables)
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow.xml'),
            os.path.join(BASE_DIR, 'expected_workflows/heavy_grouping.xml'))
        self.assertTrue(bool_test)

    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    def test_generate_incremental(self, m_eval):
        """test incremental workflow generation"""
        m_eval.return_value = [['TEST_COLUMN', 'TIMESTAMP'],
                               ['test_incr_column', 'INT']]
        table = ItTable(heavy_3_prop, self.cfg_mgr)
        self.generator.generate_incremental(table)
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow.xml'),
            os.path.join(BASE_DIR,
                         'expected_workflows/incremental_workflow.xml'))
        self.assertTrue(bool_test)

    @patch('ibis.inventor.action_builder.SqoopHelper.get_timestamp_columns',
           autospec=True)
    @patch('ibis.inventor.action_builder.ActionBuilder.get_ts_columns',
           autospec=True)
    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    def test_generate_incremental_td(self, m_eval, m_gts, m_tc):
        """test incremental workflow generation - TD"""
        m_eval.return_value = [['TEST_COLUMN', 'TIMESTAMP'],
                               ['fake_split_by', 'INT']]
        m_gts.return_value = ['TEST_COLUMN']
        m_tc.return_value = ['TEST_COLUMN']
        table = ItTable(td_fake_tablename, self.cfg_mgr)
        self.generator.generate_incremental(table)
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow.xml'),
            os.path.join(BASE_DIR,
                         'expected_workflows/incremental_ts_workflow_td.xml'))
        self.assertTrue(bool_test)

    @patch('ibis.inventor.action_builder.SqoopHelper.get_timestamp_columns',
           autospec=True)
    @patch('ibis.inventor.action_builder.ActionBuilder.get_ts_columns',
           autospec=True)
    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    def test_generate_incremental_sqlserver(self, m_eval, m_gts, m_tc):
        """test incremental workflow generation - SQLSERVER"""
        m_eval.return_value = [['TEST_column', 'TIMESTAMP'],
                               ['fake_split_by', 'INT']]
        m_gts.return_value = ['TEST_column']
        m_tc.return_value = ['TEST_column']
        table = ItTable(sqlserver_fake_tablename, self.cfg_mgr)
        self.generator.generate_incremental(table)
        _path = 'expected_workflows/incremental_ts_workflow_sqlserver.xml'
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow.xml'),
            os.path.join(BASE_DIR, _path))
        self.assertTrue(bool_test)

    @patch('ibis.inventor.action_builder.SqoopHelper.get_timestamp_columns',
           autospec=True)
    @patch('ibis.inventor.action_builder.ActionBuilder.get_ts_columns',
           autospec=True)
    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    def test_generate_incremental_sqlserver_non_dbo(self, m_eval, m_gts, m_tc):
        """test incremental workflow generation - non dbo - SQLSERVER"""
        m_eval.return_value = [['TEST_column', 'TIMESTAMP'],
                               ['fake_split_by', 'INT']]
        m_gts.return_value = ['TEST_column']
        m_tc.return_value = ['TEST_column']
        table = ItTable(sqlserver_fake_tablename_NON_DBO, self.cfg_mgr)
        self.generator.generate_incremental(table)
        _path = ('expected_workflows/'
                 'incremental_ts_workflow_sqlserver_non_dbo.xml')
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow.xml'),
            os.path.join(BASE_DIR, _path))
        self.assertTrue(bool_test)

    @patch('ibis.inventor.action_builder.SqoopHelper.get_timestamp_columns',
           autospec=True)
    @patch('ibis.inventor.action_builder.ActionBuilder.get_ts_columns',
           autospec=True)
    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    def test_generate_incremental_ora_special(self, m_eval, m_gts, m_tc):
        """test incremental workflow generation - Oracle with $ in name"""
        m_eval.return_value = [['TEST_column', 'TIMESTAMP'],
                               ['fake_split_by', 'INT']]
        m_gts.return_value = ['TEST_column']
        m_tc.return_value = ['TEST_column']
        table = ItTable(dollar_fake_tablename, self.cfg_mgr)
        self.generator.generate_incremental(table)
        _path = ('expected_workflows/'
                 'incremental_ts_workflow_ora_special_char.xml')
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow.xml'),
            os.path.join(BASE_DIR, _path))
        self.assertTrue(bool_test)

    @patch('ibis.inventor.action_builder.SqoopHelper.get_timestamp_columns',
           autospec=True)
    @patch('ibis.inventor.action_builder.ActionBuilder.get_ts_columns',
           autospec=True)
    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    def test_generate_incremental_db2(self, m_eval, m_gts, m_tc):
        """test incremental workflow generation"""
        m_eval.return_value = [['TEST_COLUMN', 'TIMESTAMP'],
                               ['fake_split_by', 'INT']]
        m_gts.return_value = ['TEST_COLUMN']
        m_tc.return_value = ['TEST_COLUMN']
        table = ItTable(db2_fake_tablename, self.cfg_mgr)
        self.generator.generate_incremental(table)
        _path = 'expected_workflows/incremental_ts_workflow_db2.xml'
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow.xml'),
            os.path.join(BASE_DIR, _path))
        self.assertTrue(bool_test)

    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch('ibis.inventor.action_builder.SqoopHelper.get_timestamp_columns',
           autospec=True)
    @patch('ibis.inventor.action_builder.ActionBuilder.get_ts_columns',
           autospec=True)
    def test_generate_incremental_ts(self, m_gts, m_tc, m_eval):
        """test incremental workflow generation - oracle"""
        m_eval.return_value = [['TEST_COLUMN', 'TIMESTAMP'],
                               ['test_incr_column', 'TIMESTAMP']]
        m_tc.return_value = ['test_incr_column']
        m_gts.return_value = ['test_incr_column']
        table = ItTable(heavy_3_prop, self.cfg_mgr)
        self.generator.generate_incremental(table)
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow.xml'),
            os.path.join(BASE_DIR,
                         'expected_workflows/incremental_ts_workflow_ora.xml'))
        self.assertTrue(bool_test)

    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    @patch('ibis.inventor.action_builder.SqoopHelper.get_timestamp_columns',
           autospec=True)
    @patch('ibis.inventor.action_builder.ActionBuilder.get_ts_columns',
           autospec=True)
    def test_generate_incremental_ts_mysql(self, m_gts, m_tc, m_eval):
        """test incremental workflow generation - mysql"""
        m_eval.return_value = [['TEST_column', 'TIMESTAMP'],
                               ['TEST_INCR_column', 'TIMESTAMP'],
                               ['mysql_split_BY', 'VARCHAR']]
        m_tc.return_value = ['TEST_INCR_column']
        m_gts.return_value = ['TEST_INCR_column']
        table = ItTable(fake_fact_tbl_prop_mysql, self.cfg_mgr)
        self.generator.generate_incremental(table)
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow.xml'),
            os.path.join(BASE_DIR,
                         'expected_workflows/'
                         'incremental_ts_workflow_mysql.xml'))
        self.assertTrue(bool_test)

    def test_generate_subworkflow(self):
        """test gen sub workflow"""
        workflows = [
            '/user/dev/oozie/workspaces/ibis/workflows/'
            'fake_database_fake_bridge_tablename.xml',
            '/user/dev/oozie/workspaces/ibis/workflows/'
            'fake_database_fake_dim_tablename.xml',
            '/user/dev/oozie/workspaces/ibis/workflows/'
            'fake_database_fake_dim_tablename.xml']
        self.generator.generate_subworkflow(workflows)
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow.xml'),
            os.path.join(BASE_DIR, 'expected_workflows/subworkflow.xml'))
        self.assertTrue(bool_test)

    def test_gen_kite_ingest_workflow(self):
        request_list = [{'source_database_name': 'database_one',
                         'source_table_name': 'table_one',
                         'hdfs_loc': '/test/hdfs/loc'},
                        {'source_database_name': 'database_one',
                         'source_table_name': 'table_two',
                         'hdfs_loc': '/test/hdfs/loc'}]

        self.generator.gen_kite_ingest_workflow(request_list)
        bool_test = self.files_equal(
            os.path.join(self.cfg_mgr.files, 'test_workflow.xml'),
            os.path.join(BASE_DIR,
                         'expected_workflows/workflow_full_kite_ingest.xml'))
        self.assertTrue(bool_test)

if __name__ == '__main__':
    unittest.main()
