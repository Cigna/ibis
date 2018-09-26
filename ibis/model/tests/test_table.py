"""ItTable tests"""
import unittest
import os
from ibis.model.table import ItTable
from ibis.utilities.config_manager import ConfigManager
from ibis.settings import UNIT_TEST_ENV
from ibis.utilities.utilities import Utilities

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class ItTableTest(unittest.TestCase):
    """Tests the functionality of the ItTable class"""

    def setUp(self):
        """setup"""
        self.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        self.it_table_dict = {
            'db_username': 'test_username', 'password_file': 'test_passwd',
            'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/fake_servicename', 'db_env': 'dev',
            'domain': 'test_domain', 'source_database_name': 'test_DB',
            'source_table_name': 'test_TABLE', 'split_by': 'test_column',
            'mappers': 3, 'connection_factories': 'org.cloudera.test',
            'fetch_size': 100, 'esp_appl_id': 'test_esp_id', 'views': 'fake_view_im'}

    def test_fields(self):
        """test fields"""
        it_obj = ItTable(self.it_table_dict, self.cfg_mgr)
        self.assertEqual(it_obj.username, 'test_username')
        self.assertEqual(it_obj.password_file, 'test_passwd')
        self.assertEqual(
            it_obj.jdbcurl, 'jdbc:oracle:thin:@//fake.oracle'
                            ':1521/fake_servicename')
        self.assertEqual(it_obj.domain, 'test_domain')
        self.assertEqual(it_obj.database, 'test_db')
        self.assertEqual(it_obj.table_name, 'test_table')
        self.assertEqual(it_obj.db_table_name, 'test_db_test_table')
        self.assertEqual(it_obj.database, 'test_db')
        self.assertEqual(it_obj.db_env, 'dev')
        self.assertEqual(it_obj.hold, 0)
        self.assertEqual(it_obj.mappers, 3)
        self.assertEqual(it_obj.target_dir,
                         'mdm/test_domain/test_db/test_table')
        self.assertEqual(it_obj.frequency_readable, 'none')
        self.assertEqual(it_obj.frequency, '000')
        self.assertEqual(it_obj.load, '001')
        self.assertEqual(it_obj.views, 'fake_view_im')

    def test_fields_default(self):
        """test fields"""
        table_dict = {
            'source_database_name': 'test_DB',
            'source_table_name': 'test_TABLE'
        }
        it_obj = ItTable(table_dict, self.cfg_mgr)
        self.assertEqual(it_obj.username, '')
        self.assertEqual(it_obj.password_file, '')
        self.assertEqual(it_obj.jdbcurl, '')
        self.assertEqual(it_obj.domain, '')
        self.assertEqual(it_obj.database, 'test_db')
        self.assertEqual(it_obj.table_name, 'test_table')
        self.assertEqual(it_obj.db_table_name, 'test_db_test_table')
        self.assertEqual(it_obj.database, 'test_db')
        self.assertEqual(it_obj.db_env, 'dev')
        self.assertEqual(it_obj.hold, 0)
        self.assertEqual(it_obj.mappers, 10)
        self.assertEqual(it_obj.target_dir,
                         'mdm//test_db/test_table')
        self.assertEqual(it_obj.frequency_readable, 'none')
        self.assertEqual(it_obj.frequency, '000')
        self.assertEqual(it_obj.load, '001')
        self.assertEqual(it_obj.load_readable, 'heavy')
        self.assertEqual(it_obj.views, '')

    def test_fields_postgres(self):
        """test fields"""
        table_dict = {
            'source_database_name': 'test_DB',
            'source_table_name': 'test_TABLE',
            'jdbcurl': 'jdbc:postgresql:thin:@//fake.postgresql:1521/'
        }
        it_obj = ItTable(table_dict, self.cfg_mgr)
        self.assertEqual(
            it_obj.connection_factories,
            'com.cloudera.sqoop.manager.DefaultManagerFactory')
        table_dict = {
            'source_database_name': 'test_DB',
            'source_table_name': 'test_TABLE',
            'jdbcurl': 'jdbc:fail:thin:@//fake.failurl:1521/',
            'load': '001'
        }
        it_obj = ItTable(table_dict, self.cfg_mgr)
        with self.assertRaises(ValueError) as context:
            it_obj.is_teradata
        self.assertTrue('Unrecognized source found' in str(context.exception))
        with self.assertRaises(ValueError) as context:
            it_obj.is_sqlserver
        self.assertTrue('Unrecognized source found' in str(context.exception))
        with self.assertRaises(ValueError) as context:
            it_obj.is_postgresql
        self.assertTrue('Unrecognized source found' in str(context.exception))
        with self.assertRaises(ValueError) as context:
            it_obj.is_mysql
        self.assertTrue('Unrecognized source found' in str(context.exception))
        with self.assertRaises(ValueError) as context:
            it_obj.is_db2
        self.assertTrue('Unrecognized source found' in str(context.exception))
        with self.assertRaises(ValueError) as context:
            val = it_obj.load
        self.assertTrue('Unrecognized source found' in str(context.exception))
        with self.assertRaises(ValueError) as context:
            val = it_obj.frequency
        self.assertTrue('Unrecognized source found' in str(context.exception))
        val = ItTable.frequency_binary_helper('')
        self.assertEqual(val, '000')
        val = ItTable.frequency_binary_helper('none')
        self.assertEqual(val, '000')
        val = ItTable.frequency_binary_helper('daily')
        self.assertEqual(val, '101')
        val = ItTable.frequency_binary_helper('biweekly')
        self.assertEqual(val, '011')
        val = ItTable.frequency_binary_helper('monthly')
        self.assertEqual(val, '010')
        val = ItTable.frequency_binary_helper('quarterly')
        self.assertEqual(val, '001')
        val = ItTable.frequency_binary_helper('yearly')
        self.assertEqual(val, '111')
        val = ItTable.load_binary_helper('')
        self.assertEqual(val, '001')
        val = ItTable.load_binary_helper('small')
        self.assertEqual(val, '100')
        val = ItTable.load_binary_helper('heavy')
        self.assertEqual(val, '001')

    def test_frequency(self):
        """test readable frequency"""
        self.it_table_dict['load'] = '101010'
        it_obj = ItTable(self.it_table_dict, self.cfg_mgr)
        self.assertEqual(it_obj.frequency_readable, 'daily')
        self.assertEqual(it_obj.load, '010')

        self.it_table_dict['load'] = '100010'
        it_obj = ItTable(self.it_table_dict, self.cfg_mgr)
        self.assertEqual(it_obj.frequency_readable, 'weekly')

        self.it_table_dict['load'] = '011010'
        it_obj = ItTable(self.it_table_dict, self.cfg_mgr)
        self.assertEqual(it_obj.frequency_readable, 'biweekly')

        self.it_table_dict['load'] = '110010'
        it_obj = ItTable(self.it_table_dict, self.cfg_mgr)
        self.assertEqual(it_obj.frequency_readable, 'fortnightly')

        self.it_table_dict['load'] = '010010'
        it_obj = ItTable(self.it_table_dict, self.cfg_mgr)
        self.assertEqual(it_obj.frequency_readable, 'monthly')

        self.it_table_dict['load'] = '001010'
        it_obj = ItTable(self.it_table_dict, self.cfg_mgr)
        self.assertEqual(it_obj.frequency_readable, 'quarterly')

        self.it_table_dict['load'] = '111010'
        it_obj = ItTable(self.it_table_dict, self.cfg_mgr)
        self.assertEqual(it_obj.frequency_readable, 'yearly')

        self.it_table_dict['load'] = ''
        it_obj = ItTable(self.it_table_dict, self.cfg_mgr)
        self.assertEqual(it_obj.frequency_load, '000001')

    def test_load(self):
        """test readable load"""
        self.it_table_dict['load'] = '101100'
        it_obj = ItTable(self.it_table_dict, self.cfg_mgr)
        self.assertEqual(it_obj.load_readable, 'small')

        self.it_table_dict['load'] = '100010'
        it_obj = ItTable(self.it_table_dict, self.cfg_mgr)
        self.assertEqual(it_obj.load_readable, 'medium')

        self.it_table_dict['load'] = '011001'
        it_obj = ItTable(self.it_table_dict, self.cfg_mgr)
        self.assertEqual(it_obj.load_readable, 'heavy')

    def test_env(self):
        """test readable load"""
        self.it_table_dict['db_env'] = 'dev'
        it_obj = ItTable(self.it_table_dict, self.cfg_mgr)
        self.assertEqual(it_obj.db_env, 'dev')

    def test_update_frequency_load(self):
        """update frequency and load"""
        it_obj = ItTable(self.it_table_dict, self.cfg_mgr)
        it_obj.frequency_readable = 'monthly'
        self.assertEqual(it_obj.frequency, '010')
        it_obj.frequency = '110'
        self.assertEqual(it_obj.frequency_readable, 'fortnightly')
        it_obj.load = '001'
        self.assertEqual(it_obj.frequency_readable, 'fortnightly')
        self.assertEqual(it_obj.load, '001')

    def test_update_views(self):
        """update views"""
        it_obj = ItTable(self.it_table_dict, self.cfg_mgr)
        # test update
        it_obj.views = 'fake_view_open|call'
        self.assertEqual(it_obj.views, 'fake_view_im|fake_view_open|call')
        self.assertTrue(set(it_obj.views_list) ==
                        set(['fake_view_im', 'fake_view_open', 'call']))
        # test duplicate
        it_obj.views = 'fake_view_open|fake_view_im|call'
        self.assertEqual(it_obj.views, 'fake_view_im|fake_view_open|call')
        self.assertTrue(set(it_obj.views_list) ==
                        set(['fake_view_im', 'fake_view_open', 'call']))

        strchk = Utilities.print_box_msg(("characters other than Alphabets "
                                          "and Pipe and underscore is not"
                                          " allowed in View"), border_char='x')

        with self.assertRaises(ValueError) as context:
            it_obj.views = 'fake_view_open| *call'
            self.assertTrue(strchk in str(context.exception))

    # def test_update_views_no_domain(self):
    #    it_obj = ItTable(self.it_table_dict, self.cfg_mgr)
        # test update with no domain
    #    with self.assertRaises(ValueError) as exp_itviews:
    #        it_obj.views = 'fake_view_open'

    def test_getting_data_source(self):
        """update views"""
        it_table_dict = {
            'db_username': 'test_username', 'password_file': 'test_passwd',
            'jdbcurl': 'jdbc:oracle:///test', 'domain': 'test_domain',
            'source_database_name': 'test_DB',
            'source_table_name': 'test_TABLE',
            'split_by': 'test_column', 'mappers': 3,
            'connection_factories': 'org.cloudera.test', 'fetch_size': 100,
            'esp_appl_id': 'test_esp_id', 'views': 'fake_view_im'
        }

        it_obj = ItTable(it_table_dict, self.cfg_mgr)
        # test Oracle
        self.assertTrue(it_obj.is_oracle)
        # test Teradata
        it_obj.jdbcurl = 'jdbc:teradata:///'
        self.assertTrue(it_obj.is_teradata)
        # test SQL Server
        it_obj.jdbcurl = 'jdbc:sqlserver:///'
        self.assertTrue(it_obj.is_sqlserver)
        # test DB2
        it_obj.jdbcurl = 'jdbc:db2:///'
        self.assertTrue(it_obj.is_db2)
        # test MYSQL
        it_obj.jdbcurl = 'jdbc:mysql://fake.mysql:3306/fake_servicename'
        self.assertTrue(it_obj.is_mysql)
        # test invalid source
        with self.assertRaises(ValueError) as context:
            it_obj.jdbcurl = 'jdbc:fakesource:///'
            it_obj.is_oracle

        self.assertTrue("Unrecognized source found in: "
                        "'jdbc:fakesource:///'" in context.exception)

    def test_connection_factories(self):
        """sqlserver windows auth"""
        it_table_dict = {
            'db_username': 'test_username', 'password_file': 'test_passwd',
            'jdbcurl': 'jdbc:jtds:sqlserver://fake.sqlserver:1433;'
                       'useNTLMv2=true;domain=fake_domain;database=qa_db',
            'domain': 'test_domain',
            'source_database_name': 'qa_db',
            'source_table_name': 'qa_table',
            'split_by': 'test_column', 'mappers': 3,
            'connection_factories': 'net.sourceforge.jtds.jdbc.Driver',
            'fetch_size': 100,
            'esp_appl_id': 'test_esp_id', 'views': 'fake_view_im'
        }
        it_obj = ItTable(it_table_dict, self.cfg_mgr)
        self.assertEquals(it_obj.connection_factories,
                          'net.sourceforge.jtds.jdbc.Driver')


if __name__ == '__main__':
    unittest.main()
else:
    loader = unittest.TestLoader()

    test_classes_to_run = [ItTableTest]
    # Create test suite
    tableSuiteTest = []
    for test_class in test_classes_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        tableSuiteTest.append(suite)
