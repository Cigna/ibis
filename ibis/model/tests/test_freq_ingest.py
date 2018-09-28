"""Tests for the Freq_Ingest class"""
import unittest
from ibis.model.freq_ingest import Freq_Ingest


class Test_freq_ingest(unittest.TestCase):
    """Tests the functionality of the Freq_Ingest class"""

    def setUp(self):
        """Set Up."""
        self.freq_ingest = Freq_Ingest()

    def test_set_view_nm(self):
        """Test view_nm and view_nm"""
        self.freq_ingest.view_nm = 'mock_view_nm'
        view_name = self.freq_ingest.view_nm
        self.assertEqual(view_name, 'mock_view_nm')

    def test_set_full_tb_nm(self):
        """Test full_tb_nm and full_tb_nm"""
        self.freq_ingest.full_tb_nm = 'mock_full_table'
        view_name = self.freq_ingest.full_tb_nm
        self.assertEqual(view_name, 'mock_full_table')

    def test_set_frequency(self):
        """Test frequency and frequency"""
        self.freq_ingest.frequency = 'mock_frequency'
        view_name = self.freq_ingest.frequency
        self.assertEqual(view_name, 'mock_frequency')

    def test_set_activator(self):
        """Test activator and activator"""
        self.freq_ingest.activator = 'mock_yes'
        view_name = self.freq_ingest.activator
        self.assertEqual(view_name, 'mock_yes')
