"""Integration test suite runner."""

import unittest
import os
import sys
from ibis.driver.tests_functional.test_driver import DriverFunctionsTest
from ibis.inventory.tests_functional.test_esp_ids_inventory import ESPTest
from lib.ingest.tests_functional.test_parquet_opt_ddl_time import \
    ParquetOptTimeFunctionsTest
from ibis.utilities.tests_functional.test_settings import SettingsTest
from ibis.utilities.tests_functional.test_it_table_generation \
    import ITTableGenTest


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


def run_tests():
    # List of test classes
    test_classes_to_run = [ESPTest, DriverFunctionsTest,
                           ParquetOptTimeFunctionsTest, SettingsTest,
                           ITTableGenTest]

    # test_classes_to_run = []
    loader = unittest.TestLoader()

    # Create test suite
    suites_list = []
    for test_class in test_classes_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)

    big_suite = unittest.TestSuite(suites_list)
    runner = unittest.TextTestRunner()
    # Run test suite
    results = runner.run(big_suite)
    if len(results.errors) != 0 or len(results.failures) != 0:
        print 'Tests failed!'
        sys.exit(1)
    else:
        print 'All tests passed.'


# Main test suite for running all of the test classes
if __name__ == '__main__':
    remove_pyc_files()
    run_tests()
