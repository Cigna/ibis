import unittest
import os
from ibis.model.hive import Hive
from ibis.utilities.config_manager import ConfigManager
from ibis.settings import UNIT_TEST_ENV

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class HiveActionFunctionsTest(unittest.TestCase):
    """Tests the functionality of the Shell Action class"""

    def setUp(self):
        self.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        self.hive_params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'hive',
            'name': 'avro_parquet',
            'ok': 'parquet_swap', 'error': 'kill',
            'script': 'parquet_live.hql',
            'job_tracker': 'my_tracker', 'name_node': 'node',
            'delete': ['foo', 'bar'],
            'mkdir': ['foo1', 'bar1'], 'job_xml': ['hive.xml'],
            'config': [{'oozie.launcher.action.main.class':
                        'org.apache.oozie.action.hadoop.Hive2Main'}],
            'param': ['InputDir=/home/tucu/input-data',
                      'OutputDir=${jobOutput}'],
            'file': ['my_file.txt'], 'archive': ['hive.zip']}
        self.my_hive = Hive(**self.hive_params)

    def test_get_script(self):
        self.assertEquals(self.hive_params['script'],
                          self.my_hive.get_script())

    def test_get_job_tracker(self):
        self.assertEquals(self.hive_params['job_tracker'],
                          self.my_hive.get_job_tracker())

    def test_get_name_node(self):
        self.assertEquals(self.hive_params['name_node'],
                          self.my_hive.get_name_node())

    def test_get_delete(self):
        self.assertEquals(self.hive_params['delete'],
                          self.my_hive.get_delete())

    def test_get_mkdir(self):
        self.assertEquals(self.hive_params['mkdir'], self.my_hive.get_mkdir())

    def test_get_job_xml(self):
        self.assertEquals(self.hive_params['job_xml'],
                          self.my_hive.get_job_xml())

    def test_get_config(self):
        self.assertEquals(self.hive_params['config'],
                          self.my_hive.get_config())

    def test_get_param(self):
        self.assertEquals(self.hive_params['param'], self.my_hive.get_param())

    def test_get_file(self):
        self.assertEquals(self.hive_params['file'], self.my_hive.get_file())

    def test_get_archive(self):
        self.assertEquals(self.hive_params['archive'],
                          self.my_hive.get_archive())

    def test_gen_hive(self):
        hive_params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'hive',
            'name': 'avro_parquet', 'ok': 'parquet_swap',
            'error': 'kill',
            'script': '${nameNode}/user/data/{target_dir}/'
                      'gen/parquet_live.hql'}
        my_hive = Hive(**hive_params)
        with open(os.path.join(BASE_DIR, 'expected/hive_parquet_live.xml'),
                  'r') as my_file:
            expected = my_file.read()
        self.assertEquals(str(my_hive.generate_action()).strip(),
                          expected.strip())


if __name__ == '__main__':
    unittest.main()
