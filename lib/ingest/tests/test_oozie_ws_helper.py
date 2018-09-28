"""Tests for oozie checks and balances"""
import unittest
import os
import json
from lib.ingest.oozie_ws_helper import OozieWSHelper, Workflow, Job, Action, \
    ChecksBalancesManager, ChecksBalances
from lib.ingest.py_hdfs import PyHDFS
from mock.mock import patch
from mock import MagicMock

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class JobFunctionsTest(unittest.TestCase):
    """tests"""

    def setUp(self):
        """setup"""
        self.test_job = open(
            os.path.join(BASE_DIR, 'fixtures/test_job.json')).read()

        self.test_action = open(
            os.path.join(BASE_DIR, 'fixtures/test_action.json')).read()

    def test_one_workflow_job(self):
        """Test a job json containing one workflow"""
        with open(os.path.join(BASE_DIR, 'fixtures/one_job.json'), 'r') as \
                content_file:
            one_job_json = content_file.read()
        one_job = Job(one_job_json)

        self.assertEquals(one_job.get_offset(), 1)
        self.assertEquals(one_job.get_total(), 1)
        self.assertEquals(one_job.get_len(), 50)
        job = Workflow(self.test_job)
        self.assertEquals(one_job.get_workflows()[0].get_app_name(),
                          job.get_app_name())
        self.assertEquals(one_job.get_workflows()[0].get_console_url(),
                          job.get_console_url())
        self.assertEquals(one_job.get_workflows(), [job])

    def test_multi_workflow_job(self):
        """Test a job json containing multiple workflow"""
        with open(os.path.join(BASE_DIR, 'fixtures/all_jobs.json'), 'r') as \
                content_file:
            multi_job_json = content_file.read()
        multi_job = Job(multi_job_json)
        self.assertEquals(multi_job.get_offset(), 1)
        self.assertEquals(multi_job.get_total(), 75)
        self.assertEquals(multi_job.get_len(), 50)
        self.assertEquals(len(multi_job.get_workflows()), 50)
        self.assertTrue(Workflow(self.test_job) in multi_job.get_workflows())

    def test_workflow(self):
        """Test a workflow job"""
        with open(os.path.join(BASE_DIR, 'fixtures/workflow_one_table.json'),
                  'r') as content_file:
            workflow_json = content_file.read()
        workflow = Workflow(workflow_json)
        self.assertEquals(workflow.get_app_name(), 'fake_mem_tablename_ora_timestamp_test')
        self.assertEquals(
            workflow.get_id(), '0000014-150929151730679-oozie-oozi-W')
        self.assertEquals(len(workflow.get_actions()), 8)
        # Test all actions parsed
        self.assertTrue(Action(
            self.test_action) in workflow.get_actions())

        # Test one of the action
        action = None
        for act in workflow.get_actions():
            if act.get_name() == "fake_mem_tablename_import":
                action = act
        self.assertEquals(
            action.get_id(),
            '0000014-150929151730679-oozie-oozi-W@fake_mem_tablename_import')
        self.assertEquals(action.get_type(), 'sqoop')


class ChecksBalancesManagerFunctionsTest(unittest.TestCase):
    """Tests the functionality of the Oozie Web Services Helper class"""

    def setUp(self):
        self.oozie_url = 'http://fake.dev.edgenode:11000/oozie/v2/'
        self.impala_host = 'fake.dev.impala'
        self.chk_bal_dir = '/user/dev/data/checks_balances'
        self.ooz = MagicMock(spec=OozieWSHelper)
        self.cb_manager = ChecksBalancesManager(self.impala_host,
                                                self.oozie_url,
                                                self.chk_bal_dir)
        with open(os.path.join(BASE_DIR, 'fixtures/workflow_one_table.json'),
                  'r') as content_file:
            workflow_json = content_file.read()
        self.sample_workflow = Workflow(workflow_json)

        with open(os.path.join(BASE_DIR, 'fixtures/workflow_multi_table.json'),
                  'r') as content_file:
            workflow_json = content_file.read()
        self.sample_workflow2 = Workflow(workflow_json)

    def test_get_distinct_workflow_tables(self):
        # Test using local json
        tables = self.cb_manager.get_distinct_workflow_tables(
            self.sample_workflow)
        self.assertEquals(tables, ['fake_mem_tablename'])

        with open(os.path.join(BASE_DIR, 'fixtures/one_job.json'), 'r') as \
                content_file:
            one_job_json = content_file.read()
        self.ooz.get_jobs.return_value = (200, Job(one_job_json))
        # Test using oozie api query
        status, jobs = self.ooz.get_jobs(
            name='fake_mem_tablename_ora_timestamp_test')  # Find job matching name

        # Job should only have one workflow job, get id
        id_val = jobs.get_workflows()[0].get_id()

        with open(os.path.join(BASE_DIR, 'fixtures/workflow_one_table.json'),
                  'r') as content_file:
            workflow_json = content_file.read()
        self.ooz.get_job.return_value = (200, Workflow(workflow_json))
        # Get workflow from job id
        status, workflow = self.ooz.get_job(id_val)
        self.assertEquals(
            self.cb_manager.get_distinct_workflow_tables(workflow), ['fake_mem_tablename'])

    def test_sort_actions(self):
        """Tests that all actions in a workflow gets sorted by table name"""
        # Test using local json one table
        sorted_actions = self.cb_manager.sort_actions(self.sample_workflow)
        self.assertTrue('fake_mem_tablename' in sorted_actions.keys())
        self.assertEquals(len(sorted_actions['fake_mem_tablename']), 6)

        # Test using local json three table
        sorted_actions = self.cb_manager.sort_actions(self.sample_workflow2)
        self.assertEquals(['fake_cost_tablename', 'fake_episode_tablename', 'fake_risk_tablename'],
                          sorted_actions.keys())
        self.assertTrue(len(sorted_actions.keys()), 3)

        # Test using oozie api query
        with open(os.path.join(BASE_DIR, 'fixtures/one_job.json'), 'r') as \
                content_file:
            one_job_json = content_file.read()
        self.ooz.get_jobs.return_value = (200, Job(one_job_json))
        status, jobs = self.ooz.get_jobs(
            name='fake_mem_tablename_ora_timestamp_test')  # Find job matching name

        with open(os.path.join(BASE_DIR, 'fixtures/workflow_one_table.json'),
                  'r') as content_file:
            workflow_json = content_file.read()
        self.ooz.get_job.return_value = (200, Workflow(workflow_json))
        # Job should only have one workflow job, get id
        id_val = jobs.get_workflows()[0].get_id()
        # Get workflow from job id
        status, workflow = self.ooz.get_job(id_val)
        sorted_actions = self.cb_manager.sort_actions(workflow)
        self.assertTrue('fake_mem_tablename' in sorted_actions.keys())
        self.assertEquals(len(sorted_actions['fake_mem_tablename']), 6)

    @patch("lib.ingest.oozie_ws_helper.ChecksBalancesManager.get_parquet_size",
           autospec=True)
    def test_get_records_special_chars(self, mocked_object):
        """Tests that all actions in a workflow gets sorted by table name
        if table has special characters in table name
        """
        mocked_object.return_value = '55'
        _path = os.path.join(
            BASE_DIR, 'fixtures/special_char_table_oozie_job.json')
        with open(_path, 'r') as content_file:
            special_job_json = content_file.read()
        wf_job = Workflow(special_job_json)
        # Test using local json one table
        cb_records = self.cb_manager.get_records(wf_job)
        self.assertEquals(len(cb_records), 1)
        cb_rec = cb_records[0]

        self.assertEquals(cb_rec.domain, 'fake_database_i')
        self.assertEquals(cb_rec.db, 'fake_database')
        self.assertEquals(cb_rec.table, 'fake_$tablename')
        self.assertEquals(
            cb_rec.ingest_timestamp, 'Thu, 22 Jun 2017 17:53:21 GMT')
        self.assertEquals(cb_rec.row_count, '72437')
        self.assertEquals(cb_rec.pull_time, 46.0)
        self.assertEquals(
            cb_rec.directory, 'mdm/fake_database_i/fake_database/fake_tablename')
        self.assertEquals(cb_rec.avro_size, '22028190')
        self.assertEquals(cb_rec.success, '0')

    @patch("lib.ingest.oozie_ws_helper.ChecksBalancesManager.get_parquet_size",
           autospec=True)
    def test_get_records(self, mocked_object):
        """Tests the implementation for getting checks_balances
        objects per table for each workflow Job"""
        # Test using local json
        mocked_object.return_value = '55'
        expected = [ChecksBalances(**{
            'parquet_size': '55',
            'ingest_timestamp': 'Wed, 30 Sep 2015 18:51:53 GMT',
            'domain': 'member', 'parquet_time': 297.0,
            'db': 'fake_database',
            'avro_size': '197519591',
            'row_count': '376248', 'pull_time': 75.0,
            'directory': 'mdm/member/fake_database/fake_mem_tablename',
            'table': 'fake_mem_tablename', 'success': '0'})]

        self.assertEquals(expected,
                          self.cb_manager.get_records(self.sample_workflow))

        mocked_object.return_value = '251'
        cb1 = ChecksBalances(**{
            'parquet_size': '251',
            'ingest_timestamp': 'Tue, 29 Sep 2015 20:14:41 GMT',
            'domain': 'member', 'parquet_time': 223.0,
            'db': 'fake_database',
            'avro_size': '1176884',
            'row_count': '2290', 'pull_time': 58.0,
            'directory': 'mdm/member/fake_database/fake_episode_tablename',
            'table': 'fake_episode_tablename',
            'success': '0'})
        cb2 = ChecksBalances(**{
            'parquet_size': '251',
            'ingest_timestamp': 'Tue, 29 Sep 2015 20:14:52 GMT',
            'domain': 'member', 'parquet_time': 308.0,
            'db': 'fake_database',
            'avro_size': '118126464',
            'row_count': '413097', 'pull_time': 63.0,
            'directory': 'mdm/member/fake_database/fake_risk_tablename',
            'table': 'fake_risk_tablename',
            'success': '0'})
        cb3 = ChecksBalances(**{
            'parquet_size': '251',
            'ingest_timestamp': 'Tue, 29 Sep 2015 20:15:01 GMT',
            'domain': 'member', 'parquet_time': 226.0,
            'db': 'fake_database',
            'avro_size': '2975049',
            'row_count': '5449', 'pull_time': 59.0,
            'directory': 'mdm/member/fake_database/fake_cost_tablename',
            'table': 'fake_cost_tablename',
            'success': '0'})
        expected = [cb3, cb1, cb2]
        self.assertEquals(expected,
                          self.cb_manager.get_records(self.sample_workflow2))

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

    @patch("subprocess.call")
    @patch("lib.ingest.oozie_ws_helper.Popen")
    def test_get_parquet_size(self, mock2, mock1):
        # Test using local json
        mock1.return_value = 0
        mock2.return_value.communicate.side_effect = [('/incr_ingest_timestamp\
        =2017-04-25 /incr_ingest_timestamp=2017-04-25 /incr_ingest_timestamp\
        =2017-04-25 /incr_ingest_timestamp=2017-04-25', 'error'),
                                                      ('2222 2222 /ingest/',
                                                       'error')]
        tables = self.cb_manager.get_parquet_size(
            "test", '/user/data/incrementals')
        self.assertEquals(tables, '2222')

    @patch("lib.ingest.oozie_ws_helper.requests.get")
    def test_check_if_workflow(self, m_get):
        response = MagicMock()
        response.status_code = 200
        response.json = MagicMock()
        with open(os.path.join(BASE_DIR,
                               'fixtures/test_job_workflow.json'), 'r') as \
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
        app_name = self.cb_manager.check_if_workflow("test")
        self.assertEqual(app_name.actions, one_action.actions)

    @patch("lib.ingest.oozie_ws_helper.requests.get")
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
        self.ooz = OozieWSHelper("testurl")
        r, _ = self.ooz.get_jobs(name='testname', user='user', group='group',
                                 status='status', offset=2, length=100,
                                 job_type='workf')
        self.assertEqual(r, 200)

    @patch("lib.ingest.oozie_ws_helper.requests.get")
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

    @patch.object(PyHDFS, "insert_update")
    @patch.object(ChecksBalancesManager, 'get_records')
    def test_update_checks_balances(self, m_get, m_pyhdfs):
        mock_return = [ChecksBalances(**{
            'parquet_size': '55',
            'ingest_timestamp': 'Wed, 30 Sep 2015 18:51:53 GMT',
            'domain': 'member', 'parquet_time': 297.0,
            'db': 'fake_database',
            'avro_size': '197519591',
            'row_count': '376248', 'pull_time': 75.0,
            'directory': 'mdm/member/fake_database/fake_mem_tablename',
            'table': 'fake_mem_tablename', 'success': '0'})]
        call_expected_param1 = '/domain=member/table=fake_database_fake_mem_tablename'
        call_expected_param2 = 'mdm/member/fake_database/fake_mem_tablename|75.0' +\
            '|197519591|Wed, 30 Sep 2015 18:51:53 GMT|297.0|55|376248' +\
            '|null|null|null|0|null'
        m_get.return_value = mock_return
        self.cb_manager.update_checks_balances('app_name')
        m_get.assert_called_once_with("app_name")
        m_pyhdfs.assert_called_once_with(call_expected_param1,
                                         call_expected_param2)


if __name__ == '__main__':
    unittest.main()
else:
    loader = unittest.TestLoader()

    test_classes_to_run = [JobFunctionsTest,
                           ChecksBalancesManagerFunctionsTest]
    # Create test suite
    oozie_ws_helper_test_suite = []
    for test_class in test_classes_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        oozie_ws_helper_test_suite.append(suite)
