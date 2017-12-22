"""Utilities tests."""
import unittest
import os
import difflib
import shutil
import json
from mock import patch, MagicMock
from ibis.utilities.utilities import Utilities
from ibis.utilities.config_manager import ConfigManager
from ibis.settings import UNIT_TEST_ENV

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class Response(object):

    def __init__(self, status_code, json_r="[]", text="true"):
        self.status_code = status_code
        self.text = text
        self.json_r = json_r
        self.err_msg = '{"message" : "Error Occurred"}'

    def json(self):
        if self.status_code == 200:
            return json.loads(self.json_r)
        else:
            return json.loads(self.err_msg)


class UtilitiesFunctionsTest(unittest.TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        cls.utilities = Utilities(ConfigManager(UNIT_TEST_ENV))
        cls.utilities_run = Utilities(ConfigManager('JENKINS', '', True))
        cls.td_tbl_weekly = {
            'load': '100001', 'mappers': 20, 'domain': 'fake_view_im',
            'target_dir': 'mdm/fake_view_im/fake_database_3/'
                          'fake_services_tablename',
            'password_file': '/user/dev/fake.password.file',
            'source_table_name': 'fake_services_tablename', 'hold': 0,
            'split_by': 'epi_id', 'fetch_size': 50000,
            'source_database_name': 'fake_database_3',
            'connection_factories': 'com.cloudera.connector.teradata.'
                                    'TeradataManagerFactory',
            'full_table_name': 'fake_view_im.'
                               'fake_database_3_fake_services_tablename',
            'db_username': 'fake_username',
            'jdbcurl': 'jdbc:teradata://fake.teradata/DATABASE='
                       'fake_database_3',
            'views': 'analytics'}
        cls.oracle_tbl_monthly = {
            'load': '010001', 'mappers': 20, 'domain': 'fake_domain',
            'target_dir': 'mdm/fake_domain/fake_database_1/fake_svc_tablename',
            'password_file': '/user/dev/fake.password.file',
            'source_table_name': 'fake_svc_tablename', 'hold': 0,
            'split_by': '',
            'fetch_size': 50000, 'source_database_name': 'fake_database_1',
            'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
            'full_table_name': 'fake_domain_fake_database_1_fake_svc_tablename',
            'db_username': 'fake_username',
            'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/fake_servicename'}
        cls.sql_tbl_quarterly = {
            'full_table_name': 'logs.fake_database_2_fake_tools_tablename', 'domain': 'logs',
            'target_dir': 'mdm/logs/fake_database_2/fake_tools_tablename',
            'split_by': 'fake_split_by', 'mappers': 10,
            'jdbcurl': 'jdbc:sqlserver://fake.sqlserver:'
                       '1433;database=fake_database_2',
            'connection_factories': 'com.cloudera.sqoop.manager.'
                                    'DefaultManagerFactory',
            'db_username': 'fake_username',
            'password_file': '/user/dev/fake.password.file',
            'load': '001100', 'fetch_size': 50000, 'hold': 0,
            'source_database_name': 'fake_database_2',
            'source_table_name': 'fake_tools_tablename'}
        cls.db2_tbl_fortnightly = {
            'full_table_name': 'rx.fake_database_4_fake_rx_tablename', 'domain': 'rx',
            'target_dir': 'mdm/rx/fake_database_4/fake_rx_tablename', 'split_by': '',
            'mappers': 1,
            'jdbcurl': 'jdbc:db2://fake.db2:50400/fake_servicename',
            'connection_factories': 'com.cloudera.sqoop.manager.'
                                    'DefaultManagerFactory',
            'db_username': 'fake_username',
            'password_file': '/user/dev/fake.password.file',
            'load': '110100', 'fetch_size': 50000, 'hold': 0,
            'source_database_name': 'fake_database_4', 'source_table_name': 'fake_rx_tablename'}
        cls.mysql_tbl_fortnightly = {
            'full_table_name': 'dashboard.fake_servicename', 'domain': 'dashboard',
            'target_dir': 'mdm/dashboard/fake_servicename/', 'split_by': '',
            'mappers': 1,
            'jdbcurl': 'jdbc:mysql://\
                        fake.mysql:3306/fake_servicename',
            'connection_factories': 'com.cloudera.sqoop.manager.'
                                    'DefaultManagerFactory',
            'db_username': 'dashboard',
            'password_file': '/user/dev/fake.password.file',
            'load': '110100', 'fetch_size': 50000, 'hold': 0,
            'source_database_name': 'dashboard',
            'source_table_name': 'fake_servicename'}
        cls.appl_ref_id_tbl = {'job_name': 'C1_FAKE_CALL_FAKE_DATABASE_DAILY',
                               'frequency': 'Daily',
                               'time': '6:00', 'string_date': 'Every Day',
                               'ksh_name': 'call_fake_database_daily',
                               'domain': 'call',
                               'db': 'fake_database', 'environment': 'DEV'}

    @classmethod
    def tearDownClass(cls):
        cls.utilities = None

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

    def test_gen_kornshell(self):
        self.utilities.cfg_mgr.saves = '/test/save/location/test_workflows/'
        self.utilities.gen_kornshell('test_workflow')
        test = os.path.join(self.utilities.cfg_mgr.files, 'test_workflow.ksh')
        expected = os.path.join(BASE_DIR, 'expected_jobs/test.ksh')
        bool_val = self.files_equal(test, expected)
        self.assertTrue(bool_val)

    def test_gen_job_properties(self):
        """test job props gen"""
        status = self.utilities.gen_job_properties('test', ['test_table'],
                                                   'TEST01')
        self.assertTrue(status)

    def test_sort_tables_by_source(self):
        expected = {'teradata': [self.td_tbl_weekly],
                    'oracle': [self.oracle_tbl_monthly],
                    'sqlserver': [self.sql_tbl_quarterly,
                                  self.sql_tbl_quarterly],
                    'db2': [self.db2_tbl_fortnightly],
                    'mysql': [self.mysql_tbl_fortnightly]}
        self.assertEquals(cmp(expected,
                              self.utilities.sort_tables_by_source(
                                  [self.db2_tbl_fortnightly,
                                   self.oracle_tbl_monthly,
                                   self.td_tbl_weekly, self.sql_tbl_quarterly,
                                   self.sql_tbl_quarterly,
                                   self.mysql_tbl_fortnightly])), 0)

    def test_sort_tables_by_domain(self):
        expected = {'fake_view_im': [self.td_tbl_weekly],
                    'fake_domain': [self.oracle_tbl_monthly],
                    'logs': [self.sql_tbl_quarterly],
                    'rx': [self.db2_tbl_fortnightly, self.db2_tbl_fortnightly]}
        self.assertEquals(cmp(expected,
                              self.utilities.sort_tables_by_domain(
                                  [self.db2_tbl_fortnightly,
                                   self.oracle_tbl_monthly,
                                   self.td_tbl_weekly, self.sql_tbl_quarterly,
                                   self.db2_tbl_fortnightly])), 0)

    def test_sort_tables_by_database(self):
        expected = {
            'fake_database_1': [self.oracle_tbl_monthly],
            'fake_database_2': [self.sql_tbl_quarterly],
            'fake_database_3': [self.td_tbl_weekly, self.td_tbl_weekly],
            'fake_database_4': [self.db2_tbl_fortnightly]}
        self.assertEquals(cmp(expected,
                              self.utilities.sort_tables_by_database(
                                  [self.td_tbl_weekly, self.oracle_tbl_monthly,
                                   self.td_tbl_weekly, self.sql_tbl_quarterly,
                                   self.db2_tbl_fortnightly])), 0)

    def test_sort_tables_by_schedule(self):
        expected = {'100': [self.td_tbl_weekly],
                    '010': [self.oracle_tbl_monthly],
                    '001': [self.sql_tbl_quarterly, self.sql_tbl_quarterly],
                    '110': [self.db2_tbl_fortnightly]}

        for schedule in self.utilities.cfg_mgr.allowed_frequencies.keys():
            if schedule not in expected.keys():
                expected[schedule] = []

        self.assertEquals(cmp(expected,
                              self.utilities.sort_tables_by_schedule(
                                  [self.td_tbl_weekly, self.oracle_tbl_monthly,
                                   self.sql_tbl_quarterly,
                                   self.db2_tbl_fortnightly,
                                   self.sql_tbl_quarterly])), 0)

    def test_clean_lines(self):
        """test clean lines"""
        test_input = ['a\n', 'b\r\n', '   ', 'c\n', '\n', '  ']
        expected = ['a', 'b', 'c']
        test_lines = Utilities.clean_lines(test_input)
        self.assertTrue(set(test_lines) == set(expected))

    def test_print_box_msg(self):
        """test print box msg"""
        _path = os.path.join(BASE_DIR, 'expected_jobs/print_box_msg.txt')
        expected_fh = open(_path)
        expected_str = expected_fh.read()
        msg = 'hello\nhelloworld\nhello'
        test_str = self.utilities.print_box_msg(msg)
        self.assertTrue(self.strings_equal(expected_str, test_str))
        _path = os.path.join(BASE_DIR, 'fixtures/log_msg.txt')
        msg = open(_path, 'r').read()
        test_str = self.utilities.print_box_msg(msg)
        _path = os.path.join(BASE_DIR, 'expected_jobs/print_box_msg_long.txt')
        expected_str = open(_path).read()
        self.assertTrue(self.strings_equal(expected_str, test_str))

    @patch('ibis.utilities.utilities.Utilities.run_subprocess', autospec=True)
    def test_put_dry_workflow(self, m_run_subprocess):
        """test put dry workflow"""
        m_run_subprocess.side_effect = (0, 0)
        self.assertTrue(self.utilities.put_dry_workflow('sample_workflow'))

    @patch('ibis.utilities.utilities.Utilities.run_subprocess', autospec=True)
    @patch('ibis.utilities.utilities.subprocess.Popen', autospec=True)
    def test_run_workflow(self, m_Popen, m_run_subprocess):
        """test run workflow"""
        m_run_subprocess.return_value = 0
        proc = MagicMock()
        proc.returncode = 0
        proc.communicate = MagicMock()
        proc.communicate.return_value = ('test_output', '')
        m_Popen.return_value = proc
        self.assertTrue(self.utilities.run_workflow('sample_workflow'))

    @patch('ibis.utilities.utilities.Utilities.put_dry_workflow',
           autospec=True)
    @patch('ibis.utilities.utilities.Utilities.chmod_files', autospec=True)
    def test_gen_dryrun_workflow(self, m_chmod_files, m_put_dry_workflow):
        """test gen dryrun workflow"""
        m_put_dry_workflow.return_value = True
        wf_file = os.path.join(BASE_DIR, 'fixtures/sample_wf.xml')
        props_file = os.path.join(
            BASE_DIR, 'fixtures/sample_wf_job.properties')
        shutil.copy(wf_file, self.utilities.cfg_mgr.files)
        shutil.copy(props_file, self.utilities.cfg_mgr.files)
        _, wf_name = self.utilities.gen_dryrun_workflow('sample_wf')
        self.assertEquals(wf_name, 'sample_wf_dryrun')

    @patch('ibis.utilities.utilities.Utilities.gen_dryrun_workflow',
           autospec=True)
    @patch('ibis.utilities.utilities.subprocess.Popen', autospec=True)
    def test_dryrun_workflow(self, m_Popen, m_gen_dryrun_workflow):
        """test dryrun workflow"""
        m_gen_dryrun_workflow.return_value = (True, 'sample_wf_dryrun')
        proc = MagicMock()
        proc.returncode = 0
        proc.communicate = MagicMock()
        proc.communicate.return_value = ('test_output', '')
        m_Popen.return_value = proc
        self.assertTrue(self.utilities.dryrun_workflow('sample_wf'))

    @patch('ibis.utilities.utilities.subprocess.Popen', autospec=True)
    def test_rm_dry_workflow(self, m_Popen):
        """test rm dry workflow"""
        proc = MagicMock()
        proc.returncode = 0
        proc.communicate = MagicMock()
        proc.communicate.return_value = ('test_output', '')
        m_Popen.return_value = proc
        self.assertEquals(self.utilities.rm_dry_workflow('sample_wf'), None)

    @patch('ibis.utilities.utilities.subprocess.Popen', autospec=True)
    @patch('ibis.utilities.oozie_helper.open')
    def test_run_xml_workflow(self, m_open, m_Popen):
        """test rm dry workflow"""
        proc = MagicMock()
        proc.returncode = 0
        proc.communicate = MagicMock()
        proc.communicate.return_value = ('test_output', '')
        m_Popen.return_value = proc
        file_h = open(os.path.join(BASE_DIR, 'fixtures/sample_wf.xml'))
        m_open.return_value = file_h
        response = MagicMock()
        response.post = MagicMock()
        response.post.status_code = 250
        response.post.json.return_value = "250"
        response.post.return_value = Response(400, text="true")
        with patch('requests.post', response):
            self.assertFalse(self.utilities_run.run_xml_workflow('sample_wf'))

    @patch('ibis.utilities.utilities.subprocess.Popen', autospec=True)
    def test_run_xml_workflow_kinit(self, m_Popen):
        """test rm dry workflow"""
        proc = MagicMock()
        proc.returncode = 10
        proc.communicate = MagicMock()
        proc.communicate.return_value = ('test_output', '')
        m_Popen.return_value = proc
        self.assertFalse(self.utilities_run.run_xml_workflow('sample_wf'))


if __name__ == '__main__':
    unittest.main()
