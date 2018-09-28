import unittest
import os
from ibis.model.shell import Shell
from ibis.utilities.config_manager import ConfigManager
from ibis.settings import UNIT_TEST_ENV

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class ShellActionFunctionsTest(unittest.TestCase):
    """Tests the functionality of the Shell Action class"""

    def setUp(self):
        self.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        self.shell_params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'shell',
            'name': 'import_prep', 'ok': 'import',
            'error': 'kill', 'execute': 'import_prep.sh',
            'delete': ['/foo', '/foo/bar'],
            'mkdir': ['/foo1', '/foo1/bar'],
            'config': [{'config1': 'val1', 'config2': 'val2'}],
            'arg': ['arg1', 'arg2', 'arg3'],
            'env_var': ['table_name=test', 'target_dir=test',
                        'database=test', 'domain=test',
                        'HADOOP_CONF_DIR=/etc/hadoop/conf'],
            'file': ['/import_prep.sh#import_prep.sh'],
            'archive': ['foo.zip'],
            'capture_output': 'true'}
        self.my_shell = Shell(**self.shell_params)

    def test_get_execute(self):
        self.assertEquals(self.shell_params['execute'],
                          self.my_shell.get_execute())

    def test_get_delete(self):
        self.assertEquals(self.shell_params['delete'],
                          self.my_shell.get_delete())

    def test_get_mkdir(self):
        self.assertEquals(self.shell_params['mkdir'],
                          self.my_shell.get_mkdir())

    def test_get_configuration(self):
        self.assertEquals(self.shell_params['config'],
                          self.my_shell.get_config())

    def test_get_arg(self):
        self.assertEquals(self.shell_params['arg'], self.my_shell.get_arg())

    def test_get_env_var(self):
        self.assertEquals(self.shell_params['env_var'],
                          self.my_shell.get_env_var())

    def test_get_file(self):
        self.assertEquals(self.shell_params['file'], self.my_shell.get_file())

    def test_get_archive(self):
        self.assertEquals(self.shell_params['archive'],
                          self.my_shell.get_archive())

    def test_get_capture_output(self):
        self.assertTrue(self.my_shell.get_capture_output())

    def test_import_prep(self):
        """Test create import_prep action"""
        params = {'cfg_mgr': self.cfg_mgr, 'action_type': 'shell',
                  'name': '{source_table_name}_import_prep',
                  'ok': 'import', 'error': 'kill', 'execute': 'import_prep.sh',
                  'env_var': ['source_database_name={source_database_name}',
                              'source_table_name={source_table_name}',
                              'target_dir={target_dir}',
                              'it_table=ibis.dev_it_table',
                              'it_table_host=fake.workflow.host',
                              'HADOOP_CONF_DIR=/etc/hadoop/conf',
                              'hdfs_ingest_path=/user/dev/oozie/workspaces/'
                              'ibis/lib/ingest/'],
                  'file': [
                      '/user/dev/oozie/workspaces/ibis/lib/ingest/'
                      'import_prep.sh#import_prep.sh']}
        my_shell = Shell(**params)
        with open(os.path.join(BASE_DIR, 'expected/shell_import_prep.xml'),
                  'r') as my_file:
            expected = my_file.read()
        self.assertEquals(str(my_shell.generate_action()).strip(),
                          expected.strip())


if __name__ == '__main__':
    unittest.main()
