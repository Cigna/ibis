import unittest
import os
from ibis.model.fork import Fork
from ibis.utilities.config_manager import ConfigManager
from ibis.settings import UNIT_TEST_ENV

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class ForkActionFunctionsTest(unittest.TestCase):
    """Tests the functionality of the Join Action class"""

    def setUp(self):
        self.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        self.fork_params = {'cfg_mgr': self.cfg_mgr, 'action_type': 'fork',
                            'name': 'pipeline1',
                            'to_nodes': ['tbl1', 'tbl2', 'tbl3', 'tbl4']}
        self.fork = Fork(**self.fork_params)

    def test_get_to_nodes(self):
        """Returns the to node value"""
        self.assertEquals(self.fork_params['to_nodes'],
                          self.fork.get_to_nodes())

    def test_gen_fork(self):
        """Tests the generated xml"""
        with open(os.path.join(BASE_DIR, 'expected/fork.xml'), 'r') as my_file:
            expected = my_file.read()
        self.assertEquals(str(self.fork.generate_action()).strip(),
                          expected.strip())


if __name__ == '__main__':
    unittest.main()
