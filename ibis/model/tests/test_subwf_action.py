import unittest
import os
from ibis.model.subwf import SubWF
from ibis.utilities.config_manager import ConfigManager
from ibis.settings import UNIT_TEST_ENV

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class SubWFActionFunctionsTest(unittest.TestCase):
    """Tests the functionality of the Shell Action class"""

    def setUp(self):
        self.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        self.swf_params = {'cfg_mgr': self.cfg_mgr, 'action_type': 'subwf',
                           'name': 'fake_model',
                           'error': "JoiningNode", 'ok': "JoiningNode",
                           'file': '/user/fake_opendev/'
                                   'fake_open_framework/84_fake_model.xml',
                           'config': 'congiguration1'}

        self.my_swf = SubWF(**self.swf_params)

    def test_get_config(self):
        """Return list of configuration mapping
        :return List[{name: _some_value, value: _some_value}]"""
        self.assertEquals(self.swf_params['config'], self.my_swf.get_config())

    def test_get_path(self):
        """Returns a list of file paths
        :return List[String]"""
        self.assertEquals(self.swf_params['file'], self.my_swf.get_path())

    def test_import_prep(self):
        """Test create import_prep action"""
        params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'subwf',
            'name': 'fake_model_run',
            'error': "JoiningNode", 'ok': "JoiningNode",
            'file': '/user/fake_opendev/fake_open_framework/84_fake_model.xml',
            'config': 'congiguration1'}
        my_swf = SubWF(**params)
        with open(os.path.join(BASE_DIR,
                               'expected/subwf.xml'), 'r') as my_file:
            expected = my_file.read()
        self.assertEquals(str(my_swf.generate_action()).strip(),
                          expected.strip())


if __name__ == '__main__':
    unittest.main()
