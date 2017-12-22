"""tests for git commands"""
import unittest
import os
from mock import patch, MagicMock
from ibis.utilities.config_manager import ConfigManager
from ibis.utilities.git_helper import GitCmd
from ibis.settings import UNIT_TEST_ENV


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class GitCmdTest(unittest.TestCase):
    """Tests the file parser"""

    def setUp(self):
        """setup"""
        self.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        self.git_obj = GitCmd(self.cfg_mgr,
                              self.cfg_mgr.git_wf_local_dir,
                              self.cfg_mgr.git_workflows_url)

    def tearDown(self):
        """tear down"""
        pass

    @patch('ibis.utilities.git_helper.os', autospec=True)
    @patch('ibis.utilities.git_helper.subprocess', autospec=True)
    def test_clone_repo(self, m_subprocess, m_os):
        """test clone_repo"""
        m_subprocess.call = MagicMock()
        m_subprocess.call.return_value = 0
        status = self.git_obj.clone_repo()
        self.assertEquals(status, True)

    @patch('ibis.utilities.git_helper.os', autospec=True)
    @patch('ibis.utilities.git_helper.subprocess', autospec=True)
    def test_checkout_branch(self, m_subprocess, m_os):
        """test checkout_branch"""
        m_subprocess.call = MagicMock()
        m_subprocess.call.return_value = 0
        status = self.git_obj.checkout_branch('dev', 'FAKEW011')
        self.assertEquals(status, True)
        status = self.git_obj.checkout_branch('int', 'FAKEW011')
        self.assertEquals(status, True)
        status = self.git_obj.checkout_branch('prod', 'FAKEW011')
        self.assertEquals(status, True)

        # test if branch already exists case
        m_subprocess.call.side_effect = [1, 0, 0]
        status = self.git_obj.checkout_branch('dev', 'FAKEW011')
        self.assertEquals(status, False)

    @patch('ibis.utilities.git_helper.os', autospec=True)
    @patch('ibis.utilities.git_helper.subprocess', autospec=True)
    def test_commit_files_push(self, m_subprocess, m_os):
        """test commit files push"""
        m_subprocess.call = MagicMock()
        m_subprocess.call.return_value = 0
        status = self.git_obj.commit_files_push('FAKEW021', 'test msg', False)
        self.assertEquals(status, True)

        # new branch
        m_subprocess.call.return_value = 0
        status = self.git_obj.commit_files_push('FAKEW021', 'test msg', True)
        self.assertEquals(status, True)

        # test no commits
        m_subprocess.call.side_effect = [0, 1]
        status = self.git_obj.commit_files_push('FAKEW021', 'test msg', False)
        self.assertEquals(status, False)

if __name__ == '__main__':
    unittest.main()
