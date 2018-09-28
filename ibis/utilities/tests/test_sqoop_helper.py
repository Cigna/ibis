"""Sqoop helper tests."""
import unittest
import os
import sys
from mock import patch, MagicMock
from ibis.utilities.sqoop_helper import SqoopHelper
from ibis.utilities.config_manager import ConfigManager
from ibis.settings import UNIT_TEST_ENV
from ibis.utilities import sqoop_helper


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class SqoopHelperFunctionsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.sqooper = SqoopHelper(ConfigManager(UNIT_TEST_ENV))

    @classmethod
    def tearDownClass(cls):
        cls.sqooper = None

    def test_get_timestamp_columns(self):
        """test get_timestamp_columns"""
        col_types = [['T1', 'AT'], ['PRIM_CLM_SYS_CD', 'VARCHAR2'],
                     ['PRIM_CLM_SYS_fake_split_by', 'VARCHAR2'],
                     ['CHNL_CD', 'VARCHAR2'], ['T2', 'DA'], ['T3', 'TS'],
                     ['EVENT_TY', 'VARCHAR2'], ['t4', 'DATE'],
                     ['EVENT_NM', 'VARCHAR2'], ['T5', 'TIMESTAMP'],
                     ['T6', 'TIMESTMP'], ['EVENT_CATEG_DESC', 'VARCHAR2'],
                     ['INDIV_ENTERPRISE_ID', 'NUMBER'], ['T7', 'DATETIME'],
                     ['ACCT_NUM', 'VARCHAR2']]

        ts_ls = self.sqooper.get_timestamp_columns(col_types, 'jdbc')
        self.assertEquals(
            cmp(ts_ls, ['T1', 'T2', 'T3', 't4', 'T5', 'T6', 'T7']), 0)

    def test_get_timestamp_columns_sqlserver(self):
        """test for sql server"""
        col_types = [['ID', 'int'], ['Region', 'nvarchar'],
                     ['CCF Effective Date', 'datetime'],
                     ['Withdrawn_Date', 'datetime'],
                     ['1st Qtr 2013 Pre JOC', 'datetime']]
        jdbc = 'jdbc:sqlserver://fake.sqlserver:5016;database=fake_database'
        ts_ls = self.sqooper.get_timestamp_columns(col_types, jdbc)
        self.assertEquals(
            cmp(ts_ls, ['CCFEffectiveDate', 'Withdrawn_Date',
                        'i_1stQtr2013PreJOC']), 0)

    def test_get_lob_columns(self):
        col_types = [['COL1', 'BLOB'], ['COL2', 'VARCHAR2'], ['COL3', 'BFILE'],
                     ['COL4', 'VARCHAR2'], ['COL5', 'NTEXT'],
                     ['COL6', 'IMAGE'], ['COL7', 'CLOB'], ['COL8', 'TEXT'],
                     ['COL9', 'DBCLOB']]

        lob_ls = self.sqooper.get_lob_columns(col_types, 'jdbc')
        self.assertEquals(
            cmp(lob_ls,
                ['COL1', 'COL3', 'COL5', 'COL6', 'COL7', 'COL8', 'COL9']), 0)

    @patch('ibis.utilities.sqoop_helper.subprocess.Popen', autospec=True)
    @patch.object(sys, 'exit', autospec=True)
    def test_get_sqoop_eval_fail(self, m_exit, m_Popen):
        """test get sqoop eval fail"""
        m_Popen.side_effect = OSError
        self.sqooper.get_sqoop_eval()
        self.assertTrue(m_exit.called)

    @patch('ibis.utilities.sqoop_helper.subprocess.Popen', autospec=True)
    @patch.object(sys, 'exit', autospec=True)
    def test_eval(self, m_exit, m_Popen):
        """test sqoop eval error"""
        proc = MagicMock()
        proc.communicate = MagicMock()
        proc.communicate.return_value = ('test_output', 'sqoop error')
        m_Popen.return_value = proc
        jdbc = ('jdbc:oracle:thin:@//fake.oracle:1521/'
                'fake_servicename')
        sql_stmt = 'select * from test.test'
        db_username = 'fake_username'
        password_file = '/user/dev/fake.password.file'

        with self.assertRaises(ValueError) as exp_cm:
            self.sqooper.eval(jdbc, sql_stmt, db_username, password_file)
        self.assertEquals(exp_cm.exception.message, 'Failed on sqoop eval!')

    def test_convert_special_chars(self):
        """test convert_special_chars"""
        _str = self.sqooper.convert_special_chars('testCol')
        self.assertEqual(_str, 'testCol')
        _str = self.sqooper.convert_special_chars('22testCol')
        self.assertEqual(_str, 'i_22testCol')
        _str = self.sqooper.convert_special_chars('testCol2')
        self.assertEqual(_str, 'testCol2')
        _str = self.sqooper.convert_special_chars('test-Col_2')
        self.assertEqual(_str, 'testCol_2')
        _str = self.sqooper.convert_special_chars('test Col 2')
        self.assertEqual(_str, 'testCol2')
        _str = self.sqooper.convert_special_chars('test[A,B,C]Col')
        self.assertEqual(_str, 'testABCCol')
        _str = self.sqooper.convert_special_chars('_testCol')
        self.assertEqual(_str, 'i_testCol')
        _str = self.sqooper.convert_special_chars('__testCol')
        self.assertEqual(_str, 'i__testCol')

    def test_get_ddl_table_view(self):
        sql_stmt = self.sqooper.get_ddl_table_view("oracle", "database", "tbl")
        sql_expected = "SELECT OBJECT_TYPE FROM ALL_OBJECTS WHERE"
        sql_db_expected = " OWNER='DATABASE' AND OBJECT_NAME='TBL'"
        expected = sql_expected + sql_db_expected
        self.assertEqual(sql_stmt, expected)

    @patch('ibis.inventor.action_builder.SqoopHelper.eval', autospec=True)
    def test_get_object_types(self, sqoop_eval):

        sqoop_eval.return_value = [['Table']]
        object_type = self.sqooper.get_object_types("database", "tbl",
                                                    "oracle", "db_username",
                                                    "password_file")
        self.assertTrue(sqoop_eval.called)
        self.assertEqual(object_type, [['Table']])

    def test_get_object_types_invalid(self):

        object_type = self.sqooper.get_object_types("database", "tbl",
                                                    "dummy", "db_username",
                                                    "password_file")
        self.assertEqual(object_type, None)

    def test_get_object_types_sqoopcache(self):

        sql_expected = "SELECT OBJECT_TYPE FROM ALL_OBJECTS WHERE"
        sql_db_expected = " OWNER='DATABASE' AND OBJECT_NAME='TBL'"
        expected = sql_expected + sql_db_expected
        sqoop_helper.SQOOP_CACHE_VIEW = {expected: True}
        object_type = self.sqooper.get_object_types("database", "tbl",
                                                    "oracle", "db_username",
                                                    "password_file")
        self.assertTrue(object_type)

    def test_get_driver(self):

        jdbc = "jdbc:jtds:sqlserver://fake.sqlserver:1433;\
               useNTLMv2=true;domain=fake_domain;database=TEST_DB"
        driver = self.sqooper.get_driver(jdbc)
        expected_driver = "net.sourceforge.jtds.jdbc.Driver"
        self.assertEqual(driver, expected_driver)

if __name__ == "__main__":
    unittest.main()
