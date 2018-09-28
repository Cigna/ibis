import unittest
import os
from ibis.model.sqoop import Sqoop
from ibis.utilities.config_manager import ConfigManager
from ibis.settings import UNIT_TEST_ENV

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class SqoopActionFunctionsTest(unittest.TestCase):
    """Tests the functionality of the Sqoop Action class"""

    def setUp(self):
        self.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        self.sqoop_params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'sqoop',
            'name': 'import', 'ok': 'ok',
            'error': 'kill', 'delete': ['foo', 'bar'],
            'mkdir': ['foo1', 'bar1'],
            'config': [{'fs.hdfs.impl.disable.cache': 'true',
                        'sqoop.connection.factories':
                        '{connection_factories}'}],
            'command': 'import',
            'arg': ['-Doraoop.timestamp.string=false', '--as-avrodatafile'],
            'job_tracker': 'track_me', 'name_node': 'node',
            'file': ['/f1.txt', '/f2.txt'],
            'archive': ['1.zip']}
        self.my_sqoop = Sqoop(**self.sqoop_params)

    def test_get_delete(self):
        self.assertEquals(self.sqoop_params['delete'],
                          self.my_sqoop.get_delete())

    def test_get_mkdir(self):
        self.assertEquals(self.sqoop_params['mkdir'],
                          self.my_sqoop.get_mkdir())

    def test_get_config(self):
        self.assertEquals(self.sqoop_params['config'],
                          self.my_sqoop.get_config())

    def test_get_command(self):
        self.assertEquals(self.sqoop_params['command'],
                          self.my_sqoop.get_command())

    def test_get_arg(self):
        self.assertEquals(self.sqoop_params['arg'], self.my_sqoop.get_arg())

    def test_get_job_tracker(self):
        self.assertEquals(self.sqoop_params['job_tracker'],
                          self.my_sqoop.get_job_tracker())

    def test_get_name_node(self):
        self.assertEquals(self.sqoop_params['name_node'],
                          self.my_sqoop.get_name_node())

    def test_get_file(self):
        self.assertEquals(self.sqoop_params['file'], self.my_sqoop.get_file())

    def test_get_archive(self):
        self.assertEquals(self.sqoop_params['archive'],
                          self.my_sqoop.get_archive())

    def test_import(self):
        """Test the xml generation of a sqoop action"""
        sqoop_params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'sqoop',
            'name': 'import', 'ok': 'ok', 'error': 'kill',
            'config': [{'fs.hdfs.impl.disable.cache': 'true',
                        'sqoop.connection.factories':
                        '{connection_factories}'}],
            'command': 'import',
            'arg': ['-Doraoop.timestamp.string=false',
                    '--as-avrodatafile', '--verbose',
                    '--connect', '{jdbcurl}', '--target-dir',
                    '/user/data/ingest/{target_dir}',
                    '--delete-target-dir', '--table',
                    '{sqoop_table}', '--username', '{db_username}',
                    '--password-file', '{password_file}', '-m',
                    '{mappers}', '--fetch-size',
                    '{fetch_size}']}
        my_sqoop = Sqoop(**sqoop_params)
        with open(os.path.join(BASE_DIR, 'expected/sqoop_import.xml'),
                  'r') as my_file:
            expected = my_file.read()
        self.assertEquals(str(my_sqoop.generate_action()).strip(),
                          expected.strip())

    def test_export(self):
        """Test the xml generation of a sqoop action"""
        sqoop_params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'sqoop',
            'name': 'export', 'ok': 'ok', 'error': 'kill',
            'config': [{'fs.hdfs.impl.disable.cache': 'true',
                        'sqoop.connection.factories':
                        '{connection_factories}'}],
            'command': 'export',
            'arg': ['--verbose', '--connect', '{jdbcUrl}',
                    '--export-dir', '{exportDir}',
                    '--update-key', '{updateKey}', '--update-mode',
                    'allowinsert',
                    '--table', '{targetTable}', '--username',
                    '{dbUserName}', '--password',
                    '{password}', '-m', '10', '--input-null-string',
                    '"\\\\N"',
                    '--input-null-non-string', '"\\\\N"',
                    '--input-fields-terminated-by',
                    '"|"', ]}

        my_sqoop = Sqoop(**sqoop_params)
        with open(os.path.join(BASE_DIR, 'expected/sqoop_export.xml'),
                  'r') as my_file:
            expected = my_file.read()
        self.assertEquals(expected.strip(),
                          str(my_sqoop.generate_action()).strip())


if __name__ == '__main__':
    unittest.main()
