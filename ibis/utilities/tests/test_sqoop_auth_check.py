"""Tests for sqoop auth test"""
import unittest
import os
from mock import patch
from ibis.utilities.sqoop_auth_check import AuthTest
from ibis.utilities.config_manager import ConfigManager
from ibis.settings import UNIT_TEST_ENV

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class AuthTestTest(unittest.TestCase):
    """Tests the file parser"""

    def setUp(self):
        self.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        jdbcurl = ('jdbc:oracle:thin:@//fake.oracle:1521/'
                   'fake_servicename')
        self.auth_test = AuthTest(self.cfg_mgr, 'fake_database', 'test_table',
                                  jdbcurl)

    def tearDown(self):
        pass

    @patch('ibis.utilities.sqoop_helper.SqoopHelper._eval', autospec=True)
    def test_verify(self, m_eval):
        """test verify"""
        m_eval.return_value = (0, '123', '')
        self.assertTrue(self.auth_test.verify('uname', 'passwd'))

    @patch('ibis.utilities.sqoop_helper.SqoopHelper._eval', autospec=True)
    def test_verify_fail(self, m_eval):
        """test verify"""
        self.auth_test.jdbcurl = ('jdbc:sqlserver://fake.sqlserver:1433;database=FAKE_DATABASE')
        m_eval.return_value = (1, '123', 'error Login failed for user !')
        self.assertFalse(self.auth_test.verify('uname', 'passwd'))

if __name__ == '__main__':
    unittest.main()
