import unittest
import os
from ibis.model.ssh import SSH
from ibis.utilities.config_manager import ConfigManager
from ibis.settings import UNIT_TEST_ENV

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class SSHActionFunctionsTest(unittest.TestCase):
    """Tests the functionality of the Shell Action class"""

    def setUp(self):
        self.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        self.ssh_params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'ssh',
            'name': 'fake_model',
            'user': 'fake_open',
            'error': 'fake_model_run_log_status_failure',
            'execute': 'exec bash /home/fake_open/scripts/fake_openSAS.sh' + ' fake_model_'
                                                  'v001_t001.sas 57 1005',
            'ok': 'fake_model_run_log_status_success',
            'host': 'fake.sas.server',
            'args': ['argument1'], 'capture_output': 'false'}

        self.my_ssh = SSH(**self.ssh_params)

    def test_get_execute(self):
        self.assertEquals(self.ssh_params['execute'],
                          self.my_ssh.get_execute())

    def get_host(self):
        self.assertEquals(self.ssh_params['host'], self.my_ssh.get_host())

    def get_user(self):
        self.assertEquals(self.ssh_params['user'], self.my_ssh.get_user())

    def test_get_args(self):
        self.assertEquals(self.ssh_params['args'], self.my_ssh.get_args())

    def test_get_capture_output(self):
        self.assertTrue(not self.my_ssh.get_capture_output())

    def test_import_prep(self):
        """Test create import_prep action"""
        params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'ssh',
            'name': 'fake_model_run',
            'user': 'fake_username',
            'error': 'fake_model_run_log_status_failure',
            'execute': 'exec bash /home/fake_open/scripts/fake_openSAS.sh' + ' fake_model_'
                                                  'v001_t001.sas 57 1005',
            'ok': 'fake_model_run_log_status_success',
            'host': 'fake.sas.server', 'args': ['argument1'],
            'capture_output': 'false'}
        my_ssh = SSH(**params)
        with open(os.path.join(BASE_DIR, 'expected/ssh.xml'), 'r') as my_file:
            expected = my_file.read()
        self.assertEquals(
            str(my_ssh.generate_action()).strip(), expected.strip())


if __name__ == '__main__':
    unittest.main()
