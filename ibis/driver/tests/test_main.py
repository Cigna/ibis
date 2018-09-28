"""unit tests for main"""
import unittest
import os
from argparse import Namespace
from StringIO import StringIO
from mock import patch, MagicMock, mock_open
import ibis.driver.main
import ibis
from ibis.utilities.config_manager import ConfigManager
from ibis.settings import UNIT_TEST_ENV
from ibis.driver.driver import Driver


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class MainFunctionsTest(unittest.TestCase):
    """tests"""

    def setUp(self):
        """Setup."""
        self.cfg_mgr = ConfigManager(UNIT_TEST_ENV)

    def test_main_00(self):
        """
        Given main with out --env expect an error
        """
        args = 'commandname '.split()

        def getitem(index):
            return args[index]
        mock_sys = MagicMock()
        mock_sys.__getitem__.side_effect = getitem
        with patch('sys.argv', mock_sys):
            with self.assertRaises(SystemExit) as cm:
                ibis.driver.main.main()
                self.assertEquals(cm.exception.code,
                                  'Environment required for ibis. '
                                  'Please specify --env argument'
                                  'and provide a environment.')

    @patch('ibis.driver.main.Driver', autospec=True)
    @patch('ibis.driver.main.get_logger', autospec=True)
    @patch('ibis.driver.main.open')
    @patch('ibis.driver.main.os', autospec=True)
    @patch('ibis.driver.main.ConfigManager', autospec=True)
    @patch('ibis.driver.main.ArgumentParser', autospec=True)
    def test_main(self, m_ap, m_cm, m_os, m_open, m_get_logger, m_driver):
        """test main"""
        cm_attrs = MagicMock()
        cm_attrs.files = MagicMock()
        cm_attrs.logs = MagicMock()
        cm_attrs.saves = MagicMock()
        m_cm.return_value = cm_attrs
        status = ibis.driver.main.main()
        self.assertEquals(status, None)

    def test_env(self):
        """
        Given main with --env expect no error
        """
        args = 'commandname --env dev'.split()

        def getitem(index):
            return args[index]
        mock_sys = MagicMock()
        mock_sys.__getitem__.side_effect = getitem
        with patch('sys.argv', mock_sys):
            try:
                ibis.driver.main.main()
            except:
                assert 'No exception expected'

    def test_auth(self):
        """
        Given --auth-test argument expect invokation
        """
        args = 'commandname --auth-test --env prod'
        '--source-db fake_database --source-table fake_dim_tablename'.split()

        def getitem(index):
            return args[index]
        mock_sys = MagicMock()
        mock_sys.__getitem__.side_effect = getitem
        with patch('sys.argv', mock_sys):
            with patch('ibis.driver.main.auth_test') as mock_auth_test:
                ibis.driver.main.auth_test(args)
                mock_auth_test.assert_called_once_with(args)

    @patch('ibis.driver.main.Driver.update_lifespan')
    @patch('ibis.driver.main.Driver.update_all_lifespan')
    @patch('sys.stdout', new_callable=StringIO)
    def test_checks_balances(self, mock_stout,
                             mock_lifespan, mock_all_lifespan):
        """
        Given --checks_balances with null arguments
        and with arguments
        """
        args = Namespace(db="", table="", update_lifespan="",
                         update_all_lifespan="")
        ibis.driver.main.checks_balances(args)
        self.assertEqual(mock_stout.getvalue(), 'Please provide\
 a --db and --table and option[--update-lifespan]\n')
        args = Namespace(db="db2", table="db2_table",
                         update_lifespan="daily", update_all_lifespan="weekly")
        if args.db and args.table and args.update_lifespan:
            ibis.driver.main.Driver.update_lifespan(args)
            mock_all_lifespan.assert_called_with(args)
        elif args.update_all_lifespan:
            ibis.driver.main.Driver.update_all_lifespan(args)
            mock_lifespan.assert_called_with(args)

    @patch('ibis.driver.main.Driver.submit_request')
    def test_submit_request(self, mock_submit_req):
        """
        Given --submit_request argument expect invokation
        """
        args = 'commandname --submit_request <path to requestfile.txt>'
        '--no-git --env prod'.split()

        def getitem(index):
            return args[index]
        mock_sys = MagicMock()
        mock_sys.__getitem__.side_effect = getitem
        with patch('sys.argv', mock_sys):
            with patch('ibis.driver.'
                       'main.submit_request') as mock_submit_request:
                ibis.driver.main.submit_request(args)
                mock_submit_request.assert_called_once_with(args)
                ibis.driver.main.Driver.submit_request(args)
                mock_submit_req.assert_called_with(args)

    def test_gen_it_table(self):
        """
        Given --gen-it-table argument expect invokation
        """
        args = '--gen-it-table <path to tables.txt>'
        '--timeout 300 --env prod'.split()

        def getitem(index):
            return args[index]
        mock_sys = MagicMock()
        mock_sys.__getitem__.side_effect = getitem
        with patch('sys.argv', mock_sys):
            with patch('ibis.driver.main.gen_it_table') as mock_gen_it_table:
                ibis.driver.main.gen_it_table(args)
                print args
                mock_gen_it_table.assert_called_with(args)

    def test_auth_test_invoke(self):
        """
        Given --auth-test argument expect invokation to driver.auth_test
        """
        args = 'commandname --auth-test --env prod --source-db fake_database'
        '--source-table fake_dim_tablename --user-name fake_username'
        '--password-file /user/dev/fake.password.file'
        '--jdbc-url jdbc:oracle:thin:@//fake.oracle:1521/'
        'fake_servicename'.split()

        def getitem(index):
            return args[index]
        mock_sys = MagicMock()
        mock_sys.__getitem__.side_effect = getitem
        with patch('sys.argv', mock_sys):
            with patch('ibis.driver.driver.'
                       'Driver.auth_test') as mock_driver_auth_test:
                ibis.driver.driver.Driver.auth_test(args)
                print args
                mock_driver_auth_test.assert_called_with(args)

    def test_run_job(self):
        """
        Given --run-job expect invokation to run_oozie_job
        """
        args = 'commandname --run-job table_workflow --env prod'.split()

        def getitem(index):
            return args[index]
        mock_sys = MagicMock()
        mock_sys.__getitem__.side_effect = getitem
        with patch('sys.argv', mock_sys):
            with patch('ibis.driver.driver.'
                       'Driver.run_oozie_job') as mock_driver_oozie_job:
                with patch('ibis.driver.main.run_job') as mock_run:
                    ibis.driver.driver.Driver.run_oozie_job(args)
                    ibis.driver.main.run_job(args)
                    mock_run.assert_called_with(args)
                    mock_driver_oozie_job.assert_called_with(args)

    def test_update_it_table(self):
        """
        Given --update-it-table expect invokation to submit_it_file
        """
        args = 'commandname --update-it-table <path to tables.txt>'
        '--env prod'.split()

        def getitem(index):
            return args[index]
        mock_sys = MagicMock()
        mock_sys.__getitem__.side_effect = getitem
        with patch('sys.argv', mock_sys):
            with patch('ibis.driver.driver.'
                       'Driver.submit_it_file') as mock_driver_submit_it_file:
                with patch('ibis.driver.main.update_it_table') \
                        as mock_update_it:
                    ibis.driver.driver.Driver.submit_it_file(args)
                    ibis.driver.main.update_it_table(args)
                    mock_driver_submit_it_file.assert_called_with(args)
                    mock_update_it.assert_called_with(args)

    def test_update_it_table_export(self):
        """
        Given --update-it-table-export expect invokation to
        export_it_file_export
        """
        args = 'commandname --update-it-table <path to tables.txt>'
        '--env prod'.split()

        def getitem(index):
            return args[index]
        mock_sys = MagicMock()
        mock_sys.__getitem__.side_effect = getitem
        with patch('sys.argv', mock_sys):
            with patch('ibis.driver.driver.'
                       'Driver.submit_it_file_export') as mock_driver_export:
                with patch('ibis.driver.main.update_it_table_export') \
                        as mock_update_it:
                    ibis.driver.driver.Driver.submit_it_file_export(args)
                    ibis.driver.main.update_it_table_export(args)
                    mock_driver_export.assert_called_with(args)
                    mock_update_it.assert_called_with(args)

    @patch('ibis.driver.driver.Driver.gen_config_workflow', autospec=True)
    def test_gen_config_workflow(self, mock_gen_config_workflow):
        """test gen_config_workflow"""
        args = [mock_open]

        def getitem(index):
            return args[index]
        ibis.driver.main.driver = Driver(self.cfg_mgr)
        mock_parse_args = MagicMock()
        mock_parse_args.gen_config_workflow = MagicMock()
        mock_parse_args.gen_config_workflow.__getitem__.side_effect = getitem
        ibis.driver.main.gen_config_workflow(mock_parse_args)
        lst = mock_gen_config_workflow.call_args_list
        self.assertEquals((lst[0][0][1]), mock_open)

    @patch('ibis.driver.driver.Driver.submit_request', autospec=True)
    def test_submit_request_2(self, mock_submit_request):
        """test submit_request"""
        ibis.driver.main.driver = Driver(self.cfg_mgr)
        mock_parse_args = MagicMock()
        mock_parse_args.submit_request = mock_open
        mock_parse_args.no_git = MagicMock()
        mock_submit_request.return_value = (True, '')
        ibis.driver.main.submit_request(mock_parse_args)
        lst = mock_submit_request.call_args_list
        self.assertEquals((lst[0][0][1]), mock_open)

    @patch('ibis.driver.main.Driver.retrieve_backup')
    @patch('sys.stdout', new_callable=StringIO)
    def test_retrieve_backup_with_db(self, mock_stdout, mock_retrieve_backup):

        """
        Given arguments for retrieve_back up
        expects print statement
        """

        args = Namespace(db="", table="")
        ibis.driver.main.retrieve_backup(args)
        self.assertEqual(mock_stdout.getvalue(),
                         '--retrieve-backup requires \
--db {name} --table {name}\n')
        args = Namespace(db="db2", table="db2_table")
        if args.db and args.table:
            ibis.driver.main.Driver.retrieve_backup(args)
            mock_retrieve_backup.assert_called_with(args)

    @patch('ibis.driver.main.Driver.export')
    @patch('sys.stdout', new_callable=StringIO)
    def test_export(self, mock_stdout, mock_export):

        """
        Given with and with out null arguments to export
        expect invokation and print statement
        """
        args = Namespace(db="", table="", to="")
        ibis.driver.main.export(args)
        expected = ("--export requires --db {db}, --table {table},"
                    " and --to {db}.{table}\n")
        self.assertEqual(mock_stdout.getvalue(), expected)
        args = Namespace(db="db2", table="table", to="db.table")
        if args.db and args.table and args.to:
            ibis.driver.main.Driver.export(args)
            mock_export.assert_called_with(args)

    @patch('ibis.driver.main.Driver.gen_prod_workflow')
    @patch('sys.stdout', new_callable=StringIO)
    def test_gen_esp_workflow(self, mock_stdout, mock_workflow):

        """
        Given null arguments and expect invokation for
        gen_prod_workflow and tests the print statement
        """

        args = Namespace(gen_esp_workflow="")
        ibis.driver.main.gen_esp_workflow(args)
        self.assertEqual(mock_stdout.getvalue(), '--gen-esp-workflow \
requires at least one ESP id.\n')
        args = Namespace(esp_id="esp_id", gen_esp_workflow="gen_esp_workflow")
        mock_workflow.return_value = ('success', 'msg')
        result = ibis.driver.main.Driver.gen_prod_workflow(args)
        self.assertEquals(result, mock_workflow.return_value)

    @patch('ibis.driver.driver.Driver.export_request')
    def test_export_request(self, mock_export):
        """
        Given main with --env expect no error
        """
        mock_export.return_value = ("Status", "Test")
        filename = BASE_DIR + '/test_resources/export_request.txt'
        args = 'commandname --env dev --export-request \
        {filename}'.format(filename=filename).split()

        def getitem(index):
            return args[index]
        mock_sys = MagicMock()
        mock_sys.__getitem__.side_effect = getitem
        with patch('sys.argv', mock_sys):
            try:
                ibis.driver.main.main()
            except:
                assert 'No exception expected'

    @patch('ibis.driver.driver.Driver.export_request')
    def test_export_request_failure(self, mock_export):
        """
        Given main with --env expect no error
        """
        mock_export.return_value = ("", "Test")
        filename = BASE_DIR + '/test_resources/export_request.txt'
        args = 'commandname --env dev --export-request \
        {filename}'.format(filename=filename).split()

        def getitem(index):
            return args[index]
        mock_sys = MagicMock()
        mock_sys.__getitem__.side_effect = getitem
        with patch('sys.argv', mock_sys):
            with self.assertRaises(SystemExit) as cm:
                ibis.driver.main.main()
                self.assertEquals(cm.exception.code,
                                  "Workflow not generated - reason: Test")


if __name__ == "__main__":
    unittest.main()
