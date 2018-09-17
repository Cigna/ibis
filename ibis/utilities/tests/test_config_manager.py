"""Test config manager for all properties files"""
import unittest
from ibis.utilities.config_manager import ConfigManager
from mock.mock import patch


class ConfigManagerTest(unittest.TestCase):
    """Test SourceTable methods."""

    def setUp(self):
        """Setup."""
        pass

    def tearDown(self):
        """Tear down."""
        pass

    @patch.object(ConfigManager, 'create_path', autospec=True)
    def test_dev_props(self, m_set):
        """test dev properties file"""
        _ = ConfigManager('dev', 'dev', True)
        self.assertTrue(isinstance(_, ConfigManager))

    @patch.object(ConfigManager, 'create_path', autospec=True)
    def test_dev_perf_props(self, m_set):
        """test dev_perf properties file"""
        _ = ConfigManager('dev_perf', 'dev_perf', True)
        self.assertTrue(isinstance(_, ConfigManager))

    @patch.object(ConfigManager, 'create_path', autospec=True)
    def test_int_props(self, m_set):
        """test int properties file"""
        _ = ConfigManager('int', 'dev', True)
        self.assertTrue(isinstance(_, ConfigManager))

    @patch.object(ConfigManager, 'create_path', autospec=True)
    def test_int_test_props(self, m_set):
        """test int_test properties file"""
        _ = ConfigManager('int_test', 'dev', True)
        self.assertTrue(isinstance(_, ConfigManager))

    @patch.object(ConfigManager, 'create_path', autospec=True)
    def test_prod_props(self, m_set):
        """test prod properties file"""
        _ = ConfigManager('prod', 'dev', True)
        self.assertTrue(isinstance(_, ConfigManager))

    @patch.object(ConfigManager, 'create_path', autospec=True)
    def test_perf_props(self, m_set):
        """test perf properties file"""
        _ = ConfigManager('perf', 'dev', True)
        self.assertTrue(isinstance(_, ConfigManager))

    @patch.object(ConfigManager, 'create_path', autospec=True)
    def test_jenkins_props(self, m_set):
        """test jenkins properties file"""
        _ = ConfigManager('jenkins', 'dev', True)
        self.assertTrue(isinstance(_, ConfigManager))

    @patch.object(ConfigManager, 'create_path', autospec=True)
    def test_unit_test_props(self, m_set):
        """test unit_test properties file"""
        _ = ConfigManager('unit_test', 'dev', True)
        self.assertTrue(isinstance(_, ConfigManager))


if __name__ == "__main__":
    unittest.main()
