#!/usr/bin/python
# this file is needed for running the egg via python
import sys
import ibis_int_tests
from ibis.driver.main import main


if __name__ == '__main__':
    if sys.argv[1] == '--run-int-tests':
        ibis_int_tests.run_tests()
    else:
        main()
