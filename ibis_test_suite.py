"""Unit test suite runner."""

import unittest
import os
import sys
from ibis.driver.tests.test_driver import DriverFunctionsTest
from ibis.driver.tests.test_main import MainFunctionsTest
from ibis.inventor.tests.test_workflow_generator \
    import WorkflowGeneratorFunctionsTest
from ibis.inventor.tests.test_action_builder import ActionBuilderFunctionsTest
from ibis.inventor.tests.test_dsl_parser import DSLParserTest
from ibis.inventory.tests.test_request_inventory \
    import request_inventory_test_suite
from ibis.inventory.tests.test_inventory import InventoryFunctionsTest
from ibis.inventory.tests.test_it_inventory import ITInventoryFunctionsTest
from ibis.inventory.tests.test_cb_inventory import CBInventoryFunctionsTest
from ibis.inventory.tests.test_perf_inventory import PerfInventoryTest
from ibis.inventory.tests.test_esp_ids_inventory \
    import ESPInventoryFunctionsTest
from ibis.model.tests.test_shell_action import ShellActionFunctionsTest
from ibis.model.tests.test_sqoop_action import SqoopActionFunctionsTest
from ibis.model.tests.test_hive_action import HiveActionFunctionsTest
from ibis.model.tests.test_freq_ingest import Test_freq_ingest
from ibis.model.tests.test_join_control import JoinActionFunctionsTest
from ibis.model.tests.test_fork_control import ForkActionFunctionsTest
from ibis.model.tests.test_ssh_action import SSHActionFunctionsTest
from ibis.model.tests.test_subwf_action import SubWFActionFunctionsTest
from ibis.model.tests.test_table import tableSuiteTest
from ibis.utilities.tests.test_utilities import UtilitiesFunctionsTest
from ibis.utilities.tests.test_sqoop_helper import SqoopHelperFunctionsTest
from ibis.utilities.tests.test_vizoozie import VizOozieTest
from ibis.utilities.tests.test_it_table_generation \
    import it_table_gen_test_suite
from ibis.utilities.tests.test_config_manager import ConfigManagerTest
from ibis.utilities.tests.test_file_parser import FileParserTest
from ibis.utilities.tests.test_sqoop_auth_check import AuthTestTest
from lib.ingest.tests.test_checks_and_balances_export \
    import TestChecksBalancesExportManager
from lib.ingest.tests.test_parquet_opt_ddl_time \
    import ParquetOptTimeFunctionsTest
from lib.ingest.tests.test_quality_assurance import qa_test_suite
from lib.ingest.tests.test_pre_quality_assurance_export \
    import qa_pre_test_suite_export
from lib.ingest.tests.test_quality_assurance_export import qa_test_suite_export
from lib.ingest.tests.test_oozie_ws_helper import oozie_ws_helper_test_suite
from lib.ingest.tests.test_zookeeper_remove_locks \
    import ZookeeperLocksFunctionsTest
from lib.ingest.tests.test_impala_utils import ImpalaUtilsFunctionsTest
from lib.ingest.tests.test_import_prep import ImportPrepFunctionsTest
from lib.ingest.tests.test_py_hdfs import PyHDFSTest


def remove_pyc_files():
    """removes .pyc files"""
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    pyc_files = []
    for root, _, filenames in os.walk(curr_dir):
        for file_name in filenames:
            if len(file_name) >= 4 and file_name[-4:] == '.pyc':
                full_file_name = os.path.join(root, file_name)
                pyc_files.append(full_file_name)
    cnt = 0
    for file_name in pyc_files:
        try:
            os.remove(file_name)
            cnt += 1
        except OSError:
            print 'Could not delete file:{0}'.format(file_name)
    print "Deleted {0}/{1} .pyc files".format(cnt, len(pyc_files))

# Main test suite for running all of the test classes
if __name__ == '__main__':
    remove_pyc_files()

    # List of test classes
    test_classes_to_run = [ActionBuilderFunctionsTest, ForkActionFunctionsTest,
                           HiveActionFunctionsTest, JoinActionFunctionsTest,
                           ShellActionFunctionsTest, SqoopActionFunctionsTest,
                           InventoryFunctionsTest, ITInventoryFunctionsTest,
                           SqoopHelperFunctionsTest, FileParserTest,
                           UtilitiesFunctionsTest, VizOozieTest,
                           ParquetOptTimeFunctionsTest,
                           DriverFunctionsTest, WorkflowGeneratorFunctionsTest,
                           ESPInventoryFunctionsTest,
                           CBInventoryFunctionsTest, ConfigManagerTest,
                           AuthTestTest, 
                           ZookeeperLocksFunctionsTest,
                           MainFunctionsTest, SubWFActionFunctionsTest,
                           SSHActionFunctionsTest, ImpalaUtilsFunctionsTest,
                           TestChecksBalancesExportManager, DSLParserTest,
                           PyHDFSTest, ImportPrepFunctionsTest,
                           PerfInventoryTest, Test_freq_ingest]

    # test_classes_to_run = []
    loader = unittest.TestLoader()

    # Create test suite
    suites_list = []
    for test_class in test_classes_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)

    # Add module level test suites here
    suites_list += qa_test_suite
    suites_list += qa_test_suite_export
    suites_list += qa_pre_test_suite_export
    suites_list += it_table_gen_test_suite
    suites_list += request_inventory_test_suite
    suites_list += tableSuiteTest
    suites_list += oozie_ws_helper_test_suite

    big_suite = unittest.TestSuite(suites_list)
    runner = unittest.TextTestRunner()
    # Run test suite
    results = runner.run(big_suite)

    if len(results.errors) != 0 or len(results.failures) != 0:
        print 'Unit tests failed!'
        print """\n\033[91m
                      ,-------------.                                    \001
                     ( Tests failed! )                         .-.       \001
                      `-------------' _                         \ \      \001
                                     (_)                         \ \     \001
                                         O                       | |     \001
                                           o                     | |     \001
                                             . /\---/\   _,---._ | |     \001
                                              /^   ^  \,'       `. ;     \001
                                             ( O   O   )           ;     \001
                                              `.=o=__,'            \     \001
                                                /         _,--.__   \    \001
                                               /  _ )   ,'   `-. `-. \   \001
                                              / ,' /  ,'        \ \ \ \  \001
                                             / /  / ,'          (,_)(,_) \001
                                            (,;  (,,)                    \001
        \033[0m"""
        sys.exit(1)
    else:
        print 'All tests passed.'
