"""Checks Balances inventory tests."""
import unittest
from ibis.inventory.cb_inventory import CheckBalancesInventory
from ibis.inventory.inventory import Inventory
from ibis.utilities.config_manager import ConfigManager
from ibis.settings import UNIT_TEST_ENV
from mock.mock import patch


class CBInventoryFunctionsTest(unittest.TestCase):
    """Test the functionality of the it inventory class.
    Tests against the fake_clm_tablename record.
    """

    @patch.object(Inventory, '_connect', autospec=True)
    def setUp(self, mock_con):
        """Setup."""
        self.inventory = CheckBalancesInventory(ConfigManager(UNIT_TEST_ENV))

    def tearDown(self):
        self.inventory = None

    @patch.object(CheckBalancesInventory, 'get_rows', return_value=[])
    def test_get(self, mock_rows):
        """Test get tables"""
        self.assertEquals([], self.inventory.get('call', 'fake_database'))

    @patch.object(CheckBalancesInventory, 'run_query', autospec=True)
    def test_update(self, mock_run):
        """Test update table"""
        tbl = {'directory': 'mdm/call/fake_database/fake_call_tablename',
               'pull_time': 120, 'avro_size': 801,
               'ingest_timestamp': '2015-06-29 11:29:02', 'parquet_time': 141,
               'parquet_size': 999, 'rows': 8, 'lifespan': 7, 'ack': 1,
               'cleaned': 0, 'current_repull': 0,
               'esp_appl_id': None, 'domain': 'call',
               'table': 'fake_database_fake_call_tablename'}
        msg = self.inventory.update(tbl)
        self.assertIn('Updated table,', msg)


if __name__ == "__main__":
    unittest.main()
