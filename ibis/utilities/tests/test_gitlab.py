"""Tests for sqoop auth test"""
import unittest
import os
from mock import patch, MagicMock
from ibis.utilities.gitlab import GitlabAPI
from ibis.utilities.config_manager import ConfigManager
from ibis.settings import UNIT_TEST_ENV

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class GitlabAPITest(unittest.TestCase):
    """Tests Gitlab API"""

    def setUp(self):
        self.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        self.gitlab = GitlabAPI(self.cfg_mgr)

    def tearDown(self):
        pass

    @patch('requests.post', autospec=True)
    def test_put_request(self, m_rpost):
        """test put_request"""
        mk_res = MagicMock()
        mk_res.status_code = 201
        mk_res.json = MagicMock()
        mk_res.json.return_value = {'json': 1}
        m_rpost.return_value = mk_res
        status, res_val = self.gitlab.put_request('/test/api/', {'key': 2})
        self.assertTrue(status)
        self.assertEqual(res_val, {'json': 1})

    @patch('requests.get', autospec=True)
    def test_get_request(self, m_rget):
        """test get request"""
        mk_res = MagicMock()
        mk_res.status_code = 201
        mk_res.json = MagicMock()
        mk_res.json.return_value = {'json': 1}
        m_rget.return_value = mk_res
        status, res_val = self.gitlab.get_request('/test/api/')
        self.assertTrue(status)
        self.assertEqual(res_val, {'json': 1})

    @patch('ibis.utilities.gitlab.GitlabAPI.get_request', autospec=True)
    def test_get_merge_request_url(self, m_gr):
        """test get merge request"""
        m_gr.return_value = (True, {'iid': 456})
        url = self.gitlab.get_merge_request_url({'id': 123})
        expected = ('http://fake.git/fake_teamname'
                    '/ibis-workflows/merge_requests/456/diffs')
        self.assertEqual(url, expected)

if __name__ == '__main__':
    unittest.main()
