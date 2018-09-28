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
    def test_unit_test_props(self, m_set):
        """test unit_test properties file"""
        _ = ConfigManager('unit_test', 'dev', True)
        self.assertTrue(isinstance(_, ConfigManager))


if __name__ == "__main__":
    unittest.main()
