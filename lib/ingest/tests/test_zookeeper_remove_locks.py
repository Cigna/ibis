"""Tests for zookeeper_remove_locks.py"""
import unittest
import os
import logging
from mock import patch, Mock
from lib.ingest.zookeeper_remove_locks import ZookeeperLocks


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class ZookeeperLocksFunctionsTest(unittest.TestCase):
    """Tests."""

    def setUp(self):
        logging.basicConfig()

    def tearDown(self):
        self.conn_mgr = None
        self.ddl_types = None

    @patch('lib.ingest.zookeeper_remove_locks.KazooClient')
    def test_zookeeper_all_children(self, m_zkclient):

        zookeeper_main_nodes = [u'fake_health_sandbox',
                                u'fake_open_scratch',
                                u'fake_database_i',
                                u'ingest',
                                u'ibis',
                                u'fake_im',
                                u'default']

        m_zkclient().get_children = Mock(return_value=zookeeper_main_nodes)

        self.zkl_locks = ZookeeperLocks("host1,host2,host3",
                                        "default",
                                        "sample_07")

        self.assertEqual(self.zkl_locks.show_all_children(),
                         zookeeper_main_nodes)

    @patch('lib.ingest.zookeeper_remove_locks.KazooClient')
    def test_zookeeper_hive_namespace(self, m_zkclient):

        zookeeper_main_nodes = [u'brokers',
                                u'zookeeper',
                                u'yarn-leader-election',
                                u'hadoop-ha',
                                u'hive_zookeeper_namespace_hive1',
                                u'isr_change_notification',
                                u'admin',
                                u'controller_epoch',
                                u'hive_zookeeper_namespace_hue1',
                                u'solr',
                                u'rmstore',
                                u'zkdtsm',
                                u'consumers',
                                u'config',
                                u'hbase']

        m_zkclient().get_children = Mock(return_value=zookeeper_main_nodes)

        self.zkl_locks = ZookeeperLocks("host1,host2,host3",
                                        "default",
                                        "sample_07")

        self.assertEqual(self.zkl_locks.get_hive_namespace(),
                         "hive_zookeeper_namespace_hive1")

    @patch('lib.ingest.zookeeper_remove_locks.KazooClient')
    def test_zookeeper_my_rec(self, m_zkclient):

        self.zkl_locks = ZookeeperLocks("host1,host2,host3",
                                        "default",
                                        "sample_07")

        self.zkl_locks.get_hive_namespace = \
            Mock(return_value="hive_zookeeper_namespace_hive1")

        m_zkclient().delete = Mock(return_value="Deleted")

        m_zkclient().get_children = \
            Mock(return_value=["LOCK-EXCLUSIVE-0000000000"])

        deep_lock = "hive_zookeeper_namespace_hive1/default/sample_07/" \
                    "source_database_name=ibis/source_table_name=ibistest"

        expected_return_message = [("Deleted hive_zookeeper_namespace_hive1/"
                                    "default/sample_07/"
                                    "source_database_name=ibis/"
                                    "source_table_name=ibistest/"
                                    "LOCK-EXCLUSIVE-0000000000")]

        self.assertEqual(self.zkl_locks.my_rec(deep_lock),
                         expected_return_message)

    @patch('lib.ingest.zookeeper_remove_locks.KazooClient')
    def test_zookeeper_my_rec_no_new_locks(self, m_zkclient):

        self.zkl_locks = ZookeeperLocks("host1,host2,host3",
                                        "default",
                                        "sample_07")

        self.zkl_locks.get_hive_namespace = \
            Mock(return_value="hive_zookeeper_namespace_hive1")

        m_zkclient().delete = Mock(return_value="Deleted")

        m_zkclient().get_children = \
            Mock(return_value=[])

        deep_lock = "hive_zookeeper_namespace_hive1/default/sample_07/" \
                    "source_database_name=ibis/source_table_name=ibistest"

        expected_return_message = "All done"

        self.assertEqual(self.zkl_locks.my_rec(deep_lock),
                         expected_return_message)

if __name__ == '__main__':
    unittest.main()
