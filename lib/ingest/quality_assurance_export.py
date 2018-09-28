"""IBIS QA.

Compares the following between source and target tables:
1. Compares row counts
"""

import sys
import os
import traceback
import logging
import datetime
import json

from abc import ABCMeta
from sqoop_utils import SqoopUtils
from impala_utils import ImpalaConnect
from checks_and_balances_export import ChecksBalancesExportManager
from py_hdfs import PyHDFS


LOG_FILE = 'qa.log'
DEBUG = False

_custom_format = "[%(levelname)s:%(filename)s:%(lineno)3s - " \
                 "%(funcName)20s() ] %(message)s"
logging.basicConfig(filename='qa1.log', filemode='w',
                    level=logging.INFO, format=_custom_format)
logger = None

ORACLE = 'oracle'
DB2 = 'db2'
TERADATA = 'teradata'
SQL_SERVER = 'sql_server'
MY_SQL = 'mysql'


class LogHandler(object):
    """log to hive table, stdout and stderr."""

    def __init__(self, host, source_table, qa_exp_results_tbl_path):
        """Init."""
        self.source_table = source_table
        self.host = host
        self.buffer_logs = []
        self.qa_status_log = ''
        self.pyhdfs = PyHDFS(qa_exp_results_tbl_path)

    def info(self, log):
        """Print to stdout."""
        self.buffer_logs.append(log)

    def error(self, log):
        """Save error results to hive."""
        self.buffer_logs.append(log)

    def qa_status(self, qa_stages):
        """
        Logs success and failure status of diffrent qa stages.
        Args:
            stages(dict): dict of status of each stage
        """
        self.qa_status_log = json.dumps(qa_stages)

    def _escape(self, msg):
        """Escape special characters."""
        _msg = msg
        _msg = _msg.replace("'", "\\'")
        _msg = _msg.replace('"', '\\"')
        _msg = _msg.replace(',', '\\,')
        _msg = _msg.replace('\n', ' ').replace('\r', '')
        return _msg

    def insert_hive(self):
        """Insert buffered logs to Hive."""
        global DEBUG

        log_txt = '\n'.join(self.buffer_logs)
        _log_txt = self._escape(log_txt.strip())
        _now = datetime.datetime.now()
        _time = _now.strftime("%Y-%m-%d %H:%M:%S.%f")
        format_data = {
            'failed_table_name': self.source_table.strip(),
            'log': _log_txt,
            'log_time': _time,
            'status': self.qa_status_log
        }
        with open(LOG_FILE, 'w') as file_h:
            file_h.write(log_txt)

        # File-based insert operation
        self.pyhdfs.insert_update('',
                                  self.prepares_data(format_data),
                                  is_append=True)

        # We need to perform INVALIDATE METADATA to reflect Hive
        # file-based operations in Impala.
        ImpalaConnect.invalidate_metadata(self.host, 'ibis.qa_export_results')

        msg = '\n' + '#' * 100
        msg += ("\nFor QA results: Run query in Impala: "
                "select status, * from ibis.qa_export_results where\
                 log_time='{0}'")
        msg += '\n' + '#' * 100
        msg = msg.format(_time)
        logging.info('Inserted logs to Hive')

    def prepares_data(self, format_data):
        return '{log_time}|{failed_table_name}|{log}|{status}'.format(
            **format_data)


class SourceTable(object):
    """Source table in Hive."""

    table = ""
    database_name = ""
    table_name = ""
    column_count = 0

    def __init__(self, source_table, host_name):
        """Init."""
        self.table = source_table
        self.host_name = host_name
        self.source_database_name, self.source_table_name = \
            source_table.split(".")

    def get_source_row_count(self):
        """Fetch row count of source table.
        Method to query the hive db and find the row count for each table
        in the source list.
        Returns a dictionary with key as table name and value as the row count
        """
        row_count = -1
        count_query = "SELECT COUNT(*) FROM {0};".format(self.table)
        output = ImpalaConnect.run_query(self.host_name, self.table,
                                         count_query)
        row_count = output[0][0]
        return int(row_count)


class TargetTable(SqoopUtils):
    """Target table representation."""

    __metaclass__ = ABCMeta

    column_count = 0
    row_count = 0

    def __init__(self, sqoop_jars, jdbc_url, connection_factories,
                 user_name, jceks, password_alias, table, target_schema):
        """Init."""
        SqoopUtils.__init__(self, sqoop_jars, jdbc_url, connection_factories,
                            user_name, jceks, password_alias)
        self.table = table
        self.database_name, self.table_name = self.table.split('.')
        self.target_schema = target_schema

        if 'db2' in jdbc_url:
            self.db_vendor = DB2
        elif 'teradata' in jdbc_url:
            self.db_vendor = TERADATA
        else:
            self.db_vendor = ORACLE

    def build_row_count_query(self):
        """Build row count query."""
        return "SELECT COUNT(*) FROM {0}".format(self.table)

    def get_target_row_count(self):
        """Fetch row count of Target table."""
        row_count = -1
        row_count_query = self.build_row_count_query()
        returncode, output, err = self.eval(row_count_query)
        if returncode == 0:
            _, row_data = self.fetch_rows_sqoop(output)
            row_count = int(row_data[0][0])
        else:
            logger.error(output)
            logger.error(err)
            raise ValueError('Error in get row count')

        self.row_count = row_count
        return row_count


class OracleTable(TargetTable):
    """Oracle specific methods."""

    TIMESTAMP = 'timestamp'
    DATE = 'date'
    DATE_TYPES = (TIMESTAMP, DATE)
    NUMBER = 'number'
    VARCHAR = 'varchar'
    VARCHAR2 = 'varchar2'
    NVARCHAR2 = 'nvarchar2'
    VARIABLE_LENGTH_COLS = [VARCHAR, VARCHAR2, NVARCHAR2]

    def __init__(self, *args):
        """Init oracle table."""
        super(self.__class__, self).__init__(*args)


class DB2Table(TargetTable):
    """DB2 specific methods."""

    TIMESTMP = 'timestmp'
    TIMESTAMP = 'timestamp'
    DATE = 'date'
    TIME = 'time'
    VARCHAR = 'varchar'
    VARIABLE_LENGTH_COLS = [VARCHAR]
    DATE_TYPES = (TIMESTMP, TIMESTAMP, DATE, TIME)

    def __init__(self, *args):
        """Init DB2 table."""
        super(self.__class__, self).__init__(*args)


class TeraDataTable(TargetTable):
    """Teradata specific methods."""

    DATE_TYPES = ()
    CV = 'cv'
    VARIABLE_LENGTH_COLS = [CV]

    def __init__(self, *args):
        """Init teradata table."""
        super(self.__class__, self).__init__(*args)


class MSSqlTable(TargetTable):
    """Microsoft SQL server specific methods."""

    VARCHAR = 'varchar'
    BIT = 'bit'
    INT = 'int'
    BIGINT = 'bigint'
    SMALLINT = 'smallint'
    TINYINT = 'tinyint'
    FLOAT = 'float'
    DECIMAL = 'decimal'
    NUMERIC = 'numeric'
    MONEY = 'money'
    DATE_TYPES = ['date', 'datetimeoffset', 'datetime2', 'smalldatetime',
                  'datetime', 'time']

    def __init__(self, *args):
        """Init MSSQL table."""
        super(self.__class__, self).__init__(*args)


class MySqlTable(TargetTable):
    """MySQL specific methods."""

    VARCHAR = 'varchar'
    BIT = 'bit'
    INT = 'int'
    BIGINT = 'bigint'
    SMALLINT = 'smallint'
    MEDIUMINT = 'medint'
    TINYINT = 'tinyint'
    FLOAT = 'float'
    DOUBLE = 'double'
    DECIMAL = 'decimal'
    NUMERIC = 'numeric'
    DATE_TYPES = ['date', 'datetime', 'time', 'timestamp', 'year']

    def __init__(self, *args):
        """Init MySQL table."""
        super(self.__class__, self).__init__(*args)


class TableValidation(object):
    """Validates the target SQL tables with Hive tables."""

    def __init__(self, source_table, target_table, params):
        """Init."""
        self.params = params
        self.source_table = source_table
        self.target_table = target_table
        self.checks_balances = ChecksBalancesExportManager(
            params['host_name'],
            params['oozie_url'],
            params['qa_exp_results_dir'])
        self.source_obj = SourceTable(self.source_table, params['host_name'])
        if 'oracle' in params['jdbc_url']:
            self.target_obj = OracleTable(
                params['jars'], params['jdbc_url'],
                params['connection_factories'], params['user_name'],
                params['jceks'], params['password_alias'], self.target_table,
                params['target_schema'])
        elif 'db2' in params['jdbc_url']:
            self.target_obj = DB2Table(
                params['jars'], params['jdbc_url'],
                params['connection_factories'], params['user_name'],
                params['jceks'], params['password_alias'], self.target_table,
                params['target_schema'])
        elif 'teradata' in params['jdbc_url']:
            self.target_obj = TeraDataTable(
                params['jars'], params['jdbc_url'],
                params['connection_factories'], params['user_name'],
                params['jceks'], params['password_alias'], self.target_table,
                params['target_schema'])
        elif 'mysql' in params['jdbc_url']:
            self.target_obj = MySqlTable(
                params['jars'], params['jdbc_url'],
                params['connection_factories'], params['user_name'],
                params['jceks'], params['password_alias'],
                self.target_table, params['target_schema'])
        else:
            self.target_obj = MSSqlTable(
                params['jars'], params['jdbc_url'],
                params['connection_factories'], params['user_name'],
                params['jceks'], params['password_alias'], self.target_table,
                params['target_schema'])
        logger.info("Evaluating {0} and {1} tables".format(self.source_table,
                                                           self.target_table))

    def count_matches(self, source_rowcount, target_rowcount):
        """Compare row count of source and target tables."""
        logger.info("Row count validation:")

        status = False
        if int(source_rowcount) == 0:
            percent = 100.0
        else:
            percent = (float(target_rowcount)/float(source_rowcount)) * 100
        if percent <= 95 or percent > 100:
            err_msg = """FAILED. Row count check, hive: {0}
            Target: {1} percent: {2}"""
            logger.error(err_msg.format(source_rowcount,
                                        target_rowcount, percent))
        else:
            info_msg = "Row count matches: hive: {0} Target: {1} percent: {2}"
            logger.info(info_msg.format(source_rowcount,
                                        target_rowcount, percent))
            status = True
        return status

    def start_data_sampling(self, action):
        """Start qa data sampling"""
        if not self.target_obj:
            return True
        logger.info('QA data sampling')
        qa_stages = {}

        source_rowcount = self.source_obj.get_source_row_count()
        sqoop_logcount = self.checks_balances.validate_in_and_out_counts(
            action)
#         target_rowcount = self.target_obj.get_target_row_count()
        self.count_bool = self.count_matches(source_rowcount, sqoop_logcount)
        qa_stages['count'] = self.count_bool

        logger.qa_status(qa_stages)
        return self.count_bool


def main():
    global logger
    args = {
        'source_database_name': sys.argv[1],
        'source_table_name': sys.argv[2],
        'database': sys.argv[3],
        'target_table': sys.argv[4],
        'jars': sys.argv[5],
        'jdbc_url': sys.argv[6],
        'connection_factories': sys.argv[7],
        'user_name': sys.argv[8],
        'jceks': sys.argv[9],
        'password_alias': sys.argv[10],
        'host_name': os.environ['IMPALA_HOST'],
        'target_schema': sys.argv[12],
        'oozie_url': sys.argv[13],
        'workflow_name': sys.argv[14],
        'qa_exp_results_dir': sys.argv[16]
    }
    DEBUG = True if sys.argv[15] == 'true' else False

    cb_mgr = ChecksBalancesExportManager(args['host_name'],
                                         args['oozie_url'],
                                         args['qa_exp_results_dir'])
    actions = cb_mgr.check_if_workflow_actions(args['workflow_name'])
    for action in actions:
        if sys.argv[2] + '_export' in action.get_name():
            new_action = action

    source_table = args['source_database_name'] + '.' + \
        args['source_table_name']
    logger = LogHandler(args['host_name'],
                        source_table,
                        args['qa_exp_results_dir'])
    try:

        target_table = args['database'] + '.' + args['target_table']
        val_obj = TableValidation(source_table, target_table, args)
        bool_status = val_obj.start_data_sampling(new_action)
        logger.insert_hive()
        exit_code = 0 if bool_status else 1
    except Exception as e:
        exit_code = 1
        logger.error(traceback.format_exc())
        logger.insert_hive()

    ImpalaConnect.close_conn()
    sys.exit(exit_code)

if __name__ == '__main__':
    if len(sys.argv) != 17:
        print "ERROR----> command line args are not proper"
        print len(sys.argv), sys.argv
        sys.exit(1)
    main()
