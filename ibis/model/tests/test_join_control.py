import unittest
import os
from ibis.model.join import Join
from ibis.utilities.config_manager import ConfigManager
from ibis.settings import UNIT_TEST_ENV

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class JoinActionFunctionsTest(unittest.TestCase):
    """Tests the functionality of the Join Action class"""

    def setUp(self):
        self.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        self.join_params = {'cfg_mgr': self.cfg_mgr,
                            'action_type': 'join', 'name': 'pipeline1_join',
                            'to_node': 'pipeline2'}
        self.join = Join(**self.join_params)

    def test_get_to_node(self):
        """Returns the to node value"""
        self.assertEquals(self.join_params['to_node'], self.join.get_to_node())

    def test_gen_join(self):
        """Tests the generated xml"""
        expected = '<join name=\"pipeline1_join\" to=\"pipeline2\"/>'
        self.assertEquals(str(self.join.generate_action()).strip(),
                          expected.strip())


if __name__ == '__main__':
    unittest.main()
