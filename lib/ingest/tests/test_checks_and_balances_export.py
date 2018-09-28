import unittest
import os
import json
from mock import MagicMock
from mock.mock import patch
from lib.ingest.checks_and_balances_export import ChecksBalancesExportManager
from lib.ingest.checks_and_balances_export import ChecksBalancesExport
from lib.ingest.checks_and_balances_export import ChecksBalancesExportHelper
from lib.ingest.py_hdfs import PyHDFS
from lib.ingest.checks_and_balances_export import Workflow, Action, Job


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class ActionTest(unittest.TestCase):
    def test_retries(self):
        retries_object = Action('{}')
        retries_object.retries = 'test'
        self.assertEqual('test', retries_object.get_retries())

    def test_transition(self):
        transition_object = Action('{}')
        transition_object.transition = 'test'
        self.assertEqual('test', transition_object.get_transition())

    def test_toString(self):
        toString_object = Action('{}')
        toString_object.toString = 'test'
        self.assertEqual('test', toString_object.get_to_string())

    def test_cred(self):
        cred_object = Action('{}')
        cred_object.cred = 'test'
        self.assertEqual('test', cred_object.get_cred())

    def test_errorMessage(self):
        errorMessage_object = Action('{}')
        errorMessage_object.errorMessage = 'test'
        self.assertEqual('test', errorMessage_object.get_error_message())

    def test_errorCode(self):
        errorCode_object = Action('{}')
        errorCode_object.errorCode = 'test'
        self.assertEqual('test', errorCode_object.get_error_code())

    def test_consoleUrl(self):
        consoleUrl_object = Action('{}')
        consoleUrl_object.consoleUrl = 'test'
        self.assertEqual('test', consoleUrl_object.get_console_url())

    def test_externalId(self):
        externalId_object = Action('{}')
        externalId_object.externalId = 'test'
        self.assertEqual('test', externalId_object.get_external_id())

    def test_externalStatus(self):
        externalStatus_object = Action('{}')
        externalStatus_object.externalStatus = 'test'
        self.assertEqual('test', externalStatus_object.get_external_status())


class TestChecksBalancesExportManager(unittest.TestCase):
    def setUp(self):
        self.host = 'fake.dev.edgenode'
        self.oozie_url = 'http://fake.dev.edgenode:11000/oozie/v2/'
        self.chk_bal_exp_dir = '/user/dev/data/checks_balances'
        self.ooz = MagicMock(spec=ChecksBalancesExportHelper)
        self.cb_manager = ChecksBalancesExportManager(self.host,
                                                      self.oozie_url,
                                                      self.chk_bal_exp_dir)
        with open(os.path.join(BASE_DIR, 'fixtures/workflow_export.json'),
                  'r') as content_file:
            workflow_json = content_file.read()
        self.sample_workflow = Workflow(workflow_json)

        with open(os.path.join(BASE_DIR, 'fixtures/workflow_export.json'),
                  'r') as content_file:
            workflow_json = content_file.read()
        self.sample_workflow2 = Workflow(workflow_json)

    @patch.object(ChecksBalancesExportHelper, "get_jobs")
    def test_get_distinct_workflow_tables(self, mock1):
        # Test using local json
        tables = self.cb_manager.get_distinct_workflow_tables(
            self.sample_workflow)
        self.assertEquals(tables, ['test'])

        with open(os.path.join(BASE_DIR, 'fixtures/one_job.json'), 'r') as \
                content_file:
            one_job_json = content_file.read()
        self.ooz.get_jobs.return_value = (200, Job(one_job_json))
        # Test using oozie api query
        status, jobs = self.ooz.get_jobs(
            name='fake_mem_tablename_ora_timestamp_test')  # Find job matching name

        # Job should only have one workflow job, get id
        id_val = jobs.get_workflows()[0].get_id()

        with open(os.path.join(BASE_DIR, 'fixtures/workflow_export.json'),
                  'r') as content_file:
            workflow_json = content_file.read()
        self.ooz.get_job.return_value = (200, Workflow(workflow_json))
        # Get workflow from job id
        status, workflow = self.ooz.get_job(id_val)
        self.assertEquals(
            self.cb_manager.get_distinct_workflow_tables(workflow), ['test'])

    @patch.object(ChecksBalancesExportHelper, "get_jobs")
    @patch.object(ChecksBalancesExportHelper, "get_job")
    def test_get_workflow_jobs(self, mock2, mock1):
        # Test using local json
        with open(os.path.join(BASE_DIR, 'fixtures/one_job.json'), 'r') as \
                content_file:
            one_job_json = content_file.read()
        mock1.return_value = (200, Job(one_job_json))
        mock2.return_value = (200, Workflow(one_job_json))
        tables = self.cb_manager.get_workflow_job(
            "test")
        print tables
        self.assertEquals(tables, Workflow(one_job_json))

    @patch.object(ChecksBalancesExportHelper, "get_jobs")
    @patch.object(ChecksBalancesExportHelper, "get_job")
    def test_get_workflow_jobs_N(self, mock2, mock1):
        # Test using local json
        with open(os.path.join(BASE_DIR, 'fixtures/one_job.json'), 'r') as \
                content_file:
            one_job_json = content_file.read()
        mock1.return_value = (500, Job(one_job_json))
        mock2.return_value = (500, Workflow(one_job_json))
        tables = self.cb_manager.get_workflow_job(
            "test")
        print tables
        self.assertFalse(tables, Workflow(one_job_json))

    @patch("lib.ingest.checks_and_balances_export.requests.get")
    def test_get_workflow_job(self, m_get):
        response = MagicMock()
        response.status_code = 200
        response.json = MagicMock()
        with open(os.path.join(BASE_DIR,
                               'fixtures/multiworkflow.json'), 'r') as \
                content_file:
            one_job_json = content_file.read()
        with open(os.path.join(BASE_DIR,
                               'fixtures/test_job_action.json'), 'r') as \
                content_file:
            one_action_json = content_file.read()
        one_action = Workflow(one_action_json)
        response.json.side_effect = [json.loads(one_job_json),
                                     json.loads(one_action_json)]
        m_get.return_value = response
        app_name = self.cb_manager.get_workflow_job("test",
                                                    '2015-09-30T19:00:29.000Z')
        self.assertEqual(app_name.actions, one_action.actions)

    @patch.object(ChecksBalancesExportHelper, "get_jobs")
    @patch.object(ChecksBalancesExportHelper, "get_job")
    @patch('lib.ingest.checks_and_balances_export.Workflow.get_actions')
    def test_check_if_workflow(self, mock3, mock2, mock1):
        # Test using local json
        with open(os.path.join(BASE_DIR, 'fixtures/one_job.json'), 'r') as \
                content_file:
            one_job_json = content_file.read()
        with open(os.path.join(BASE_DIR, 'fixtures/test_job1.json'), 'r') as \
                content_file:
            one_action_json = content_file.read()
        mock1.return_value = (200, Job(one_job_json))
        mock2.return_value = (200, Workflow(one_job_json))
        mock3.return_value = [Action(one_action_json)]
        self.actions = [{'test': 'test'}]
        tables = self.cb_manager.check_if_workflow(
            "test")
        print tables
        self.assertEquals(tables, Workflow(one_job_json))

    @patch.object(ChecksBalancesExportHelper, "get_jobs")
    @patch.object(ChecksBalancesExportHelper, "get_job")
    @patch('lib.ingest.checks_and_balances_export.Workflow.get_actions')
    def test_check_if_workflow_actions(self, mock3, mock2, mock1):
        # Test using local json
        with open(os.path.join(BASE_DIR, 'fixtures/one_job.json'), 'r') as \
                content_file:
            one_job_json = content_file.read()
        with open(os.path.join(BASE_DIR, 'fixtures/test_job1.json'), 'r') as \
                content_file:
            one_action_json = content_file.read()
        mock1.return_value = (200, Job(one_job_json))
        mock2.return_value = (200, Workflow(one_job_json))
        mock3.return_value = [Action(one_action_json)]
        self.actions = [{'test': 'test'}]
        tables = self.cb_manager.check_if_workflow_actions(
            "test")
        print tables
        self.assertTrue(tables, Workflow(one_job_json))

    @patch("subprocess.call")
    @patch("lib.ingest.checks_and_balances_export.Popen")
    def test_get_parquet_size(self, mock2, mock1):
        # Test using local json
        mock1.return_value = 0
        mock2.return_value.communicate.return_value = ('/incr_ingest_timestamp\
        =2017-04-25 /incr_ingest_timestamp=2017-04-25 /incr_ingest_timestamp\
        =2017-04-25 /incr_ingest_timestamp=2017-04-25', 'error')
        tables = self.cb_manager.get_parquet_size(
            "test", '/user/data/incrementals')
        self.assertEquals(tables, 0)

    def test_sort_actions(self):
        """Tests that all actions in a workflow gets sorted by table name"""
        # Test using local json one table
        sorted_actions = self.cb_manager.sort_actions(self.sample_workflow)
        self.assertTrue('test' in sorted_actions.keys())
        self.assertEquals(len(sorted_actions['test']), 6)

        # Test using local json three table
        sorted_actions = self.cb_manager.sort_actions(self.sample_workflow2)
        self.assertEquals(['test'],
                          sorted_actions.keys())
        self.assertTrue(len(sorted_actions.keys()), 3)
        # Test using oozie api query
        with open(os.path.join(BASE_DIR, 'fixtures/one_job.json'), 'r') as \
                content_file:
            one_job_json = content_file.read()
        self.ooz.get_jobs.return_value = (200, Job(one_job_json))
        status, jobs = self.ooz.get_jobs(
            name='fake_mem_tablename_ora_timestamp_test')  # Find job matching name

        with open(os.path.join(BASE_DIR, 'fixtures/workflow_export.json'),
                  'r') as content_file:
            workflow_json = content_file.read()
        self.ooz.get_job.return_value = (200, Workflow(workflow_json))
        # Job should only have one workflow job, get id
        id_val = jobs.get_workflows()[0].get_id()
        # Get workflow from job id
        status, workflow = self.ooz.get_job(id_val)
        sorted_actions = self.cb_manager.sort_actions(workflow)
        self.assertTrue('test' in sorted_actions.keys())
        print len(sorted_actions['test'])
        self.assertEquals(len(sorted_actions['test']), 6)

    @patch.object(ChecksBalancesExportManager, "get_parquet_size")
    def test_get_records(self, mocked_object):
        """Tests the implementation for getting checks_balances
        objects per table for each workflow Job"""
        # Test using local json
        mocked_object.return_value = '55'
        expected = [ChecksBalancesExport(**{
            'parquet_size': '55',
            'export_timestamp': 'Tue, 07 Feb 2017 05:23:09 GMT',
            'domain': 'test_oracle_export', 'parquet_time': 0,
            'db': 'test_oracle_export',
            'txt_size': '4394', 'row_count': '376248',
            'push_time': 63.0, 'table_name': 'test',
            'directory': 'mdm//test_oracle_export/test', 'success': '0'})]

        self.assertEquals(expected,
                          self.cb_manager.get_records(self.sample_workflow))

        mocked_object.return_value = '55'
        cb1 = ChecksBalancesExport(**{
            'parquet_size': '55',
            'export_timestamp': 'Tue, 07 Feb 2017 05:23:09 GMT',
            'domain': 'test_oracle_export', 'parquet_time': 0,
            'db': 'test_oracle_export',
            'txt_size': '4394', 'row_count': '376248',
            'push_time': 23.0, 'table_name': 'test',
            'directory': 'mdm//test_oracle_export/test', 'success': '0'})
#         cb2 = ChecksBalancesExport(**{
#             'parquet_size': '55',
#             'export_timestamp': 'Tue, 07 Feb 2017 05:23:09 GMT',
#             'domain': 'test_oracle_export', 'parquet_time': 0,
#             'db': 'test_oracle_export',
#             'txt_size': '4394', 'row_count': '376248',
#             'push_time': 58.0, 'table_name': 'test',
#             'directory': 'mdm//test_oracle_export/test', 'success': '0'})
#         cb3 = ChecksBalancesExport(**{
#             'parquet_size': '55',
#             'export_timestamp': 'Tue, 07 Feb 2017 05:23:09 GMT',
#             'domain': 'test_oracle_export', 'parquet_time': 0,
#             'db': 'test_oracle_export',
#             'txt_size': '4394', 'row_count': '376248',
#             'push_time': 43.0, 'table_name': 'test',
#             'directory': 'mdm//test_oracle_export/test', 'success': '0'})
        expected = [cb1]
        self.assertEquals(isinstance(expected, ChecksBalancesExport),
                          isinstance(self.cb_manager.get_records
                                     (self.sample_workflow2),
                                     ChecksBalancesExport))

    def test_success_or_failure(self):
        """
        Validate that if not all 'OK' in the status
        of actions, 2 is returned. Else 0
        :return: 0 or 2 depending on if actions suceeded.
        These are used in checks & balances for reporting on if a
        workflow suceeded or not.
        """
        action1 = Action(open(
            os.path.join(BASE_DIR, 'fixtures/test_action1.json')).read())

        action2 = Action(open(
            os.path.join(BASE_DIR, 'fixtures/test_action2.json')).read())

        action_list = [action1]
        self.assertEquals('0',
                          self.cb_manager.get_success_or_failure(action_list))
        action_list = [action1, action2]
        self.assertEquals('2',
                          self.cb_manager.get_success_or_failure(action_list))

    @patch.object(ChecksBalancesExportManager, "get_records")
    @patch(
        "lib.ingest.py_hdfs.PyHDFS.execute",
        return_value=(
            'Success',
            0,
            None))
    @patch(
        "lib.ingest.py_hdfs.PyHDFS.execute_with_echo",
        return_value=(
            'Success',
            0,
            None))
    def test_update_checks_balances(self, mock_records, mock1, mock2):
        checks = ChecksBalancesExport('domain', 'db', 'table_name',
                                      export_timestamp='', row_count=0,
                                      push_time=0, directory='null',
                                      txt_size=0, parquet_time=0,
                                      parquet_size=0, success='1')
        return_val = []
        return_val.append(checks)
        mock1.return_value = return_val
        self.cb_manager.update_checks_balances("workflow")

    @patch("lib.ingest.checks_and_balances_export.requests.get")
    def test_get_jobs(self, m_get):
        response = MagicMock()
        response.status_code = 200
        response.json = MagicMock()
        with open(os.path.join(BASE_DIR,
                               'fixtures/test_job_workflow.json'), 'r') as \
                content_file:
            one_job_json = content_file.read()
        response.json.return_value = json.loads(one_job_json)
        m_get.return_value = response
        self.ooz = ChecksBalancesExportHelper("testurl")
        r, _ = self.ooz.get_jobs(name='testname', user='user', group='group',
                                 status='status', offset=2, length=100,
                                 job_type='workf')
        self.assertEqual(r, 200)

    @patch.object(PyHDFS, "insert_update")
    @patch.object(ChecksBalancesExportManager, 'get_records')
    def test_update_checks_balances_N(self, m_get, m_pyhdfs):
        mock_return = [ChecksBalancesExport(**{
            'parquet_size': '55',
            'export_timestamp': 'Wed, 30 Sep 2015 18:51:53 GMT',
            'domain': 'member', 'parquet_time': 297.0,
            'db': 'fake_database',
            'row_count': '376248', 'push_time': 75.0,
            'directory': 'mdm/member/fake_database/fake_mem_tablename',
            'table_name': 'fake_mem_tablename', 'success': '0'})]
        call_expected_param1 = '/domain=member/table_name=fake_mem_tablename'
        call_expected_param2 = 'mdm/member/fake_database/fake_mem_tablename|75.0' +\
            '|0|Wed, 30 Sep 2015 18:51:53 GMT|297.0|55|376248' +\
            '|null|null|null|0|null'
        m_get.return_value = mock_return
        self.cb_manager.update_checks_balances('app_name')
        m_get.assert_called_once_with("app_name")
        m_pyhdfs.assert_called_once_with(call_expected_param1,
                                         call_expected_param2)

if __name__ == '__main__':
    unittest.main()
