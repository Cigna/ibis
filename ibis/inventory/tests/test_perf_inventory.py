"""Tests for perf_inventory"""
import os
import unittest
import getpass

from mock import patch, Mock

from ibis.inventory.perf_inventory import PerfInventory
from ibis.settings import UNIT_TEST_ENV
from ibis.utilities.config_manager import ConfigManager
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

mock_wipe_table = [['mock_view_nm|mock_team', 'mock_src_tbl',
                    'mock_src_db', 'mock_full_table']]


class PerfInventoryTest(unittest.TestCase):

    """Tests the functionality of the PerfInventory class"""

    def setUp(self):
        """Set Up."""
        self.perf_inventory = PerfInventory(ConfigManager(UNIT_TEST_ENV))
        self.cfg_mgr = ConfigManager(UNIT_TEST_ENV)

    @patch.object(PerfInventory, 'run_ingest_hql')
    @patch.object(PerfInventory, 'run_query')
    @patch.object(PerfInventory, 'get_rows')
    @patch.object(PerfInventory, 'reingest_hql')
    def test_wipe_perf_env(self, m_reingest_hql, m_get_rows, m_run_query,
                          m_run_hql):
        """test wipe_perf_env"""
        m_get_rows.return_value = mock_wipe_table
        m_reingest_hql.return_value = 'abc.hql'
        self.perf_inventory.wipe_perf_env('mock_view_nm', True)
        get_row = ("select views, source_table_name, source_database_name, "
                   "full_table_name from ibis.dev_it_table where views "
                   "like '%mock_view_nm%';")
        m_get_rows.assert_called_once_with(get_row)
        m_run_hql.assert_called_once_with('abc.hql')

    @patch.object(PerfInventory, 'run_query')
    @patch.object(PerfInventory, 'get_rows')
    def test_wipe_perf_env_noreingest(self, m_get_rows, m_run_query):
        """ test wipe_perf_env without reingest"""
        self.perf_inventory.wipe_perf_env('test_team', False)
        query = ' create database test_team '
        m_run_query.assert_called_with(query)

    @patch.object(PerfInventory, 'run_query')
    def test_insert_freq_ingest(self, m_run_query):
        """ test insert in freq_ingest table without activator
        """
        self.perf_inventory.insert_freq_ingest(['fake_view_im'],
                                              ['weekly'],
                                              ['fake_database_mock_table'],
                                              ['default'])
        insert_query = ("insert overwrite table ibis.freq_ingest partition"
                        "(view_name, full_table_name) values ('weekly', "
                        "'yes', 'fake_view_im', 'fake_database_mock_table')")
        freq_ingest = "ibis.freq_ingest"
        m_run_query.assert_called_once_with(insert_query, freq_ingest)

    @patch.object(PerfInventory, 'run_query')
    @patch.object(PerfInventory, 'get_rows')
    def test_insert_freq_ingest_wd(self, m_get_rows, m_run_query):
        """ test insert in freq_ingest table with activator
        """
        self.perf_inventory.insert_freq_ingest(['fake_view_im'],
                                              ['weekly'],
                                              ['fake_database_mock_table'],
                                              ['no'])
        insert_query = ("insert overwrite table ibis.freq_ingest partition"
                        "(view_name, full_table_name) values ('weekly', "
                        "'no', 'fake_view_im', 'fake_database_mock_table')")
        freq_ingest = "ibis.freq_ingest"
        get_row = ("select * from ibis.freq_ingest where view_name='fake_view_im' "
                   "and full_table_name='fake_database_mock_table'")
        m_get_rows.assert_called_once_with(get_row)
        m_run_query.assert_called_once_with(insert_query, freq_ingest)

    @patch.object(PerfInventory, 'run_query', autospec=True)
    def test_reingest_hql(self, m_run_query):
        """test reingest HQL"""
        file_name = 'test_view_{0}_full_src_table.hql'.format(
            getpass.getuser())
        expected_hql_nm = os.path.join(self.cfg_mgr.files, file_name)
        actual_hql_nm = self.perf_inventory.reingest_hql('test_view',
                                                        'src_table',
                                                        'src_db',
                                                        'full_table')
        with open(actual_hql_nm, 'r') as file_h:
            actual_hql = file_h.read()

        with open(BASE_DIR + '/expected/reingest_hql.hql', 'r') as file_h:
            expected_hql = file_h.read()

        self.assertTrue(expected_hql_nm, actual_hql_nm)
        self.assertTrue(expected_hql, actual_hql)

    @patch('ibis.inventory.perf_inventory.subprocess.Popen')
    def test_run_ingest_hql_p(self, mock_subproc_popen):
        """test run_ingest_hql"""
        process_mock = Mock()
        attrs = {'communicate.return_value': ('output', 'error'),
                 'returncode':  0}
        process_mock.configure_mock(**attrs)
        mock_subproc_popen.return_value = process_mock
        self.perf_inventory.run_ingest_hql('abc.hql')
        jdbc_url = "-u 'jdbc:hive2://test-fake.hive:25006/default'"
        expected_value = ['beeline', '/etc/hive/beeline.properties',
                          jdbc_url,
                          " -f 'abc.hql'"]
        expected_value = " ".join(expected_value)
        mock_subproc_popen.assert_called_once_with(expected_value, shell=True)

    @patch('ibis.inventory.perf_inventory.subprocess.Popen')
    def test_run_ingest_hql_n(self, mock_subproc_popen):
        """test run_ingest_hq else partl"""
        process_mock = Mock()
        attrs = {'communicate.return_value': ('output', 'error'),
                 'returncode':  1}
        process_mock.configure_mock(**attrs)
        mock_subproc_popen.return_value = process_mock
        self.perf_inventory.run_ingest_hql('abc.hql')
        jdbc_url = "-u 'jdbc:hive2://test-fake.hive:25006/default'"
        expected_value = ['beeline', '/etc/hive/beeline.properties',
                          jdbc_url,
                          " -f 'abc.hql'"]
        expected_value = " ".join(expected_value)
        mock_subproc_popen.assert_called_once_with(expected_value, shell=True)
