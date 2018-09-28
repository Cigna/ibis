"""IBIS QA.

Compares the following between source and target tables:
1. Compares DDL

It also does:
Validates DDL
"""

import sys
import os
import traceback
import logging
import datetime
import json
from abc import ABCMeta, abstractmethod
from itertools import izip

import sql_queries
from sqoop_utils import SqoopUtils
from impala_utils import ImpalaConnect
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
POSTGRESQL = 'postgresql'


class LogHandler(object):
    """log to hive table, stdout and stderr. """

    def __init__(self, source_table, host_name, qa_exp_results_tbl_path):
        """Init."""
        self.source_table = source_table
        self.host_name = host_name
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
        with open(LOG_FILE, 'wb') as file_h:
            file_h.write(log_txt)

        # File-based insert operation
        self.pyhdfs.insert_update('', self.prepares_data(format_data),
                                  is_append=True)

        # We need to perform INVALIDATE METADATA to reflect Hive
        # file-based operations in Impala.
        ImpalaConnect.invalidate_metadata(self.host_name,
                                          'ibis.qa_export_results')

        msg = '\n' + '#' * 100
        msg += ("\nFor QA results: Run query in Impala: "
                "select status, * from ibis.qa_export_results where "
                "log_time='{0}'")
        msg += '\n' + '#' * 100
        msg = msg.format(_time)
        print msg
        logging.info('Inserted logs to Hive')

    def prepares_data(self, format_data):
        return '{log_time}|{failed_table_name}|{log}|{status}'.format(
            **format_data)


class ColumnDDL(object):
    """Data Definition Language representation of table/columns."""

    def __init__(self, column_name, data_type, data_len):
        """Init."""
        self.column_name = column_name.lower()
        self.data_type = data_type.lower()
        self.data_len = data_len

    @property
    def data_len(self):
        """Getter data_len."""
        return self._data_len

    @data_len.setter
    def data_len(self, val):
        """Setter data_len."""
        if val is None:
            self._data_len = None
        elif ',' in val:
            self._precision, self._scale = val.split(',')
            self._precision, self._scale = \
                int(self._precision), int(self._scale)
            self._data_len = self._precision
        else:
            self._data_len = int(val)

    @property
    def precision(self):
        """Getter precision."""
        if hasattr(self, '_precision'):
            return self._precision
        else:
            return None

    @property
    def scale(self):
        """Getter scale."""
        if hasattr(self, '_scale'):
            return self._scale
        else:
            return None

    def __repr__(self):
        """Object str."""
        return '\nColumnDDL: {%-20s %-9s %s(%s,%s)}' % \
               (self.column_name, self.data_type, self.data_len,
                self.precision, self.scale)


class SourceTable(object):
    """Source table in Hive."""

    table = ""
    database_name = ""
    table_name = ""
    column_count = 0
    DECIMAL = 'decimal'
    INT = 'int'
    TINYINT = 'tinyint'
    SMALLINT = 'smallint'
    BIGINT = 'bigint'
    MEDIUMINT = 'medint'
    FLOAT = 'float'
    DOUBLE = 'double'
    NUMERIC_TYPES = (DECIMAL, INT, TINYINT, SMALLINT, BIGINT, FLOAT, DOUBLE)
    NON_DECIMAL_NUM_TYPES = (INT, TINYINT, SMALLINT, BIGINT, FLOAT, DOUBLE)

    CHAR = 'char'
    STRING = 'string'
    VARCHAR = 'varchar'
    STRING_TYPES = (CHAR, STRING, VARCHAR)

    DATE = 'date'
    TIMESTAMP = 'timestamp'
    DATE_TYPES = (DATE, TIMESTAMP)

    BOOLEAN = 'boolean'
    BINARY = 'binary'
    MISC_TYPES = (BOOLEAN, BINARY)
    # hive types which are of variable length
    VARIABLE_LENGTH_COLS = [VARCHAR]
    # hive types which dont have length property
    NON_LEN_TYPES = MISC_TYPES + DATE_TYPES + (STRING, INT, TINYINT, SMALLINT,
                                               BIGINT, FLOAT, DOUBLE)
    # hive types that have length property
    LEN_TYPES = (VARCHAR, CHAR, DECIMAL)

    def __init__(self, source_table, host_name):
        """Init."""
        self.table = source_table
        self.host_name = host_name
        self.database_name, self.table_name = \
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

    def build_ddl_query(self):
        """Build hive ddl query."""
        return "describe {table};".format(**{'table': self.table})

    def set_column_props(self, ddl):
        """Set column names and count."""
        ddl_list = []

        # ignore two items since Hive tables have an additional
        # columns for ingest time
        column_ddl = [col_ddl for col_ddl in ddl if col_ddl[0]
                      not in ['incr_ingest_timestamp', 'ingest_timestamp']]

        for column_info in column_ddl:
            info = column_info[1].replace(')', '')
            info = info.split('(')

            if info[0] in SourceTable.NON_LEN_TYPES:
                # certain fields dont have a data len. So initiate to None
                info.append(None)
            ddl_obj = ColumnDDL(column_info[0], info[0], info[1])
            ddl_list.append(ddl_obj)

        self.column_count = len(ddl_list)
        return ddl_list

    def get_ddl(self):
        """Query hive database for column name, data type, and column count.
        Returns a dictionary of key as table name and values as column
        name and datatype
        """
        ddl_list = []
        ddl_query = self.build_ddl_query()
        output = ImpalaConnect.run_query(self.host_name, self.table, ddl_query)
        # print 'output', output
        ddl_list = self.set_column_props(output)
        # sort the list of ddl objects as source and target ddl output
        # are not ordered
        # ddl_list.sort(key=lambda ddl: ddl.column_name)
        return ddl_list


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
        self.schema = self.target_schema
        self.table_name = self.table_name.upper()

        if 'db2' in jdbc_url:
            self.db_vendor = DB2
            self.database_name = self.database_name.upper()
        elif 'teradata' in jdbc_url:
            self.db_vendor = TERADATA
        elif 'sqlserver' in jdbc_url:
            self.db_vendor = SQL_SERVER
        elif 'postgresql' in jdbc_url:
            self.db_vendor = POSTGRESQL
        elif 'mysql' in jdbc_url:
            self.db_vendor = MY_SQL
        else:
            self.db_vendor = ORACLE
            self.database_name = self.database_name.upper()

    @abstractmethod
    def build_row_count_query(self):
        """Build row count query."""
        query_params = {'database_name': self.database_name,
                        'schema_name': self.target_schema,
                        'table_name': self.table_name}
        return query_params

    def get_target_row_count(self):
        """Fetch row count of Target table."""
        row_count = -1
        row_count_query = self.build_row_count_query()
        returncode, output, err = self.eval(row_count_query)
        if returncode == 0:
            _, row_data = self.fetch_rows_sqoop(output)
            row_count = int(row_data[0][0])
        else:
            logger.error(row_count_query)
            logger.error(output)
            logger.error(err)
            raise ValueError('Error in get row count')

        self.row_count = row_count
        return row_count

    @abstractmethod
    def build_ddl_query(self):
        """Abstract method.
        Build DDL query for respective databases. Implements bit of
        functionality
        """
        query_params = {'database_name': self.database_name,
                        'table_name': self.table_name}
        return query_params

    def convert_special_chars(self, column_name):
        """convert special characters"""
        column_name = column_name.replace(' ', '').replace('(', '')
        column_name = column_name.replace(')', '').replace(',', '')
        return column_name

    def set_column_props(self, ddl_rows, increment_column):
        """Set source column names and count."""
        ddl_list = []

        for column_info in ddl_rows:
            if column_info[0] != increment_column:
                column_name = self.convert_special_chars(column_info[0])
                data_type = column_info[1]
                data_len = column_info[2]
                ddl_list.append(ColumnDDL(column_name, data_type, data_len))

        self.column_count = len(ddl_list)
        return ddl_list

    def get_ddl(self, filter_columns=None):
        """Fetch source db ddl."""
        ddl_query = self.build_ddl_query()
        returncode, output, err = self.eval(ddl_query)
        # print 'source output', output
        if returncode == 0:
            column_labels, row_data = self.fetch_rows_sqoop(
                output, filter_columns=filter_columns)
        else:
            logger.error(ddl_query)
            logger.error(output)
            logger.error(err)
            raise ValueError('Error in get ddl')
        return column_labels, row_data

    def get_ddl_list(self, row_data, increment_column=None):
        return self.set_column_props(row_data, increment_column)

    @abstractmethod
    def build_increment_query(self):
        """Build increment query."""
        query_params = {'table_name': self.table_name,
                        'database_name': self.database_name.upper()}
        return query_params

    def get_target_increment_column(self):
        increment_column = []
        increment_query = self.build_increment_query()
        returncode, output, err = self.eval(increment_query)
        if returncode == 0:
            _, row_data = self.fetch_rows_sqoop(output)
            if row_data:
                increment_column = (row_data[0][0])
            else:
                increment_column = False
        else:
            increment_column = False
            logger.info("Error in get increment column")
#             logger.error(increment_query)
#             logger.error(output)
#             logger.error(err)
#             raise ValueError('Error in get increment column')

        self.increment_column = increment_column
        return increment_column

    def is_null(self, col_val):
        """Check if the sql column value is null"""
        return 'null' in col_val

    @abstractmethod
    def build_row_column_query(self):
        """Build row count query."""
        query_params = {'database_name': self.database_name,
                        'schema_name': self.target_schema,
                        'table_name': self.table_name}
        return query_params

    def get_ddl_column(self, filter_columns=None):
        ddl_query = self.build_row_column_query()
        returncode, output, err = self.eval(ddl_query)
        # print 'source output', output
        if returncode == 0:
            column_labels, row_data = self.fetch_rows_sqoop(
                output, filter_columns=filter_columns)
        else:
            logger.error(output)
            logger.error(err)
            raise ValueError('Error in get ddl_columns')
        return column_labels, row_data


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
    TYPE_MAPPING = {
        SourceTable.STRING: ['blob', 'clob', 'nclob',
                             VARCHAR2, VARCHAR, NVARCHAR2],
        SourceTable.CHAR: ['char', 'nchar'], SourceTable.TIMESTAMP: DATE_TYPES,
        SourceTable.INT: (NUMBER), SourceTable.TINYINT: (NUMBER),
        SourceTable.BIGINT: (NUMBER), SourceTable.SMALLINT: (NUMBER),
        SourceTable.FLOAT: ['binary_float', 'float'],
        SourceTable.DOUBLE: ['binary_double', 'double', 'float', NUMBER],
        SourceTable.DECIMAL: [NUMBER], SourceTable.BINARY: ['raw'],
        SourceTable.VARCHAR: ['blob', 'clob', 'nclob', 'char', 'nchar',
                              TIMESTAMP, DATE, NUMBER, VARCHAR2, VARCHAR,
                              NVARCHAR2],
        SourceTable.BOOLEAN: [VARCHAR, VARCHAR2, NVARCHAR2]}

    def __init__(self, *args):
        """Init oracle table."""
        super(self.__class__, self).__init__(*args)

    def set_column_props(self, ddl_rows, increment_column):
        """Set source column names and count."""
        ddl_list = []

        for column_info in ddl_rows:
            column_name = column_info[0]
            data_type = column_info[1]

            if column_info[0] != increment_column:

                if self.NUMBER.upper() in data_type:
                    precision = column_info[3]
                    scale = column_info[4]
                    if self.is_null(precision):
                        precision = self.get_max_len(column_name)
                    if self.is_null(scale):
                        scale = 0
                    data_len = "{0},{1}".format(precision, scale)
                else:
                    data_len = column_info[2]

                if self.TIMESTAMP.upper() in data_type:
                    # oracle specific
                    data_type = data_type.replace(')', '')
                    data_type, data_len = data_type.split('(')

                column_name = self.convert_special_chars(column_name)
                ddl_list.append(ColumnDDL(column_name, data_type, data_len))
            else:
                ddl_list

        self.column_count = len(ddl_list)
        return ddl_list

    def get_max_len(self, column_name):
        """Fetch max len of column"""
        max_len = -1
        query = "SELECT MAX(LENGTH({0})) FROM {1}".format(column_name,
                                                          self.table)
        returncode, output, err = self.eval(query)
        if returncode == 0:
            _, rows = self.fetch_rows_sqoop(output, strip_col_val=True)
            result = rows[0][0]
            if self.is_null(result):
                # if null set it to max of hive decimal precision - 38
                max_len = 38
            else:
                max_len = int(result)
        else:
            logger.error(err)
            raise ValueError(err)
        return max_len

    def build_ddl_query(self):
        """Build DDL query."""
        query_params = super(self.__class__, self).build_ddl_query()
        query = sql_queries.ORACLE['ddl']
        query = query.format(**query_params)
        return query

    def build_row_count_query(self):
        """Build row count query."""
        query_params = super(self.__class__, self).build_row_count_query()
        query = sql_queries.ORACLE['count']
        query = query.format(**query_params)
        return query

    def build_increment_query(self):
        """Build increment check query."""
        query_params = super(self.__class__, self).build_increment_query()
        query = sql_queries.ORACLE['increment']
        query = query.format(**query_params)
        return query

    def build_row_column_query(self):
        """Build row count query."""
        query_params = super(self.__class__, self).build_row_column_query()
        query = sql_queries.ORACLE['column']
        query = query.format(**query_params)
        return query

    def check_ddl_null(self, column_label, row_data):
        """check db ddl null column for N."""
        index_null = column_label.index('NULLABLE')
        return not bool(list(column_data for column_data in row_data
                             if column_data[index_null].upper() != 'Y'))


class DB2Table(TargetTable):
    """DB2 specific methods."""

    TIMESTMP = 'timestmp'
    TIMESTAMP = 'timestamp'
    DATE = 'date'
    TIME = 'time'
    VARCHAR = 'varchar'
    VARIABLE_LENGTH_COLS = [VARCHAR]
    DATE_TYPES = (TIMESTMP, TIMESTAMP, DATE, TIME)
    TYPE_MAPPING = {
        SourceTable.STRING: [TIME, VARCHAR],
        SourceTable.VARCHAR: [VARCHAR, TIME, DATE, TIMESTAMP, 'char',
                              'integer', 'boolean', 'integer', 'smallint',
                              'bigint', 'decimal', 'numeric', 'double',
                              'real'],
        SourceTable.CHAR: ['char'], SourceTable.BOOLEAN: ['boolean', 'char'],
        SourceTable.TIMESTAMP: [TIMESTAMP, TIMESTMP, DATE],
        SourceTable.INT: ['integer'], SourceTable.SMALLINT: ['smallint'],
        SourceTable.BIGINT: ['bigint'], SourceTable.DECIMAL: ['decimal',
                                                              'numeric'],
        SourceTable.DOUBLE: ['double'], SourceTable.FLOAT: ['real']}

    def __init__(self, *args):
        """Init DB2 table."""
        super(self.__class__, self).__init__(*args)

    def build_ddl_query(self):
        """Build DDL query."""
        query_params = super(self.__class__, self).build_ddl_query()
        query = sql_queries.DB2['ddl']
        query = query.format(**query_params)
        return query

    def build_row_count_query(self):
        """Build row count query."""
        query_params = super(self.__class__, self).build_row_count_query()
        query = sql_queries.DB2['count']
        query = query.format(**query_params)
        return query

    def build_increment_query(self):
        """Build increment check query."""
        query_params = super(self.__class__, self).build_increment_query()
        query = sql_queries.DB2['increment']
        query = query.format(**query_params)
        return query

    def build_row_column_query(self):
        """Build row count query."""
        query_params = super(self.__class__, self).build_row_column_query()
        query = sql_queries.DB2['column']
        query = query.format(**query_params)
        return query

    def check_ddl_null(self, column_label, row_data):
        """check db ddl null column for N."""
        index_null = column_label.index('NULLS')
        return not bool(list(column_data for column_data in row_data
                             if column_data[index_null].upper() != 'Y'))


class TeraDataTable(TargetTable):
    """Teradata specific methods."""

    DATE_TYPES = ()
    CV = 'cv'
    VARIABLE_LENGTH_COLS = [CV]
    TYPE_MAPPING = {
        SourceTable.STRING: ['a1', 'an', 'bf', 'bo', 'bv', 'co', 'dh',
                             'dm', 'ds', 'dy', 'hm', 'hs',
                             'hr', 'jn', 'mi', 'mo', 'ms', 'pd', 'pm',
                             'ps', 'pt', 'pz', 'sc', 'sz',
                             'tz', 'ut', 'xm', 'ym', 'yr', '++', 'cv'],
        SourceTable.CHAR: ['cf'],
        SourceTable.VARCHAR: ['a1', 'an', 'bf', 'bo', 'bv',
                              'dm', 'ds', 'dy' 'hm', 'hs', 'co', 'dh',
                              'hr', 'jn', 'mi', 'mo', 'ms', 'pd', 'pm',
                              'ps', 'pt', 'pz', 'sc', 'sz', 'cf',
                              'cv', 'f', 'd', 'i1', 'i2', 'i8', 'i'],
        SourceTable.TIMESTAMP: ['at', 'da', 'ts'],
        SourceTable.DECIMAL: ['d'], SourceTable.INT: ['i1', 'i', 'n'],
        SourceTable.DOUBLE: ['f'], SourceTable.TINYINT: ['i1'],
        SourceTable.SMALLINT: ['i2'], SourceTable.BIGINT: ['i8'],
        SourceTable.FLOAT: ['f'], SourceTable.BOOLEAN: ['cf']}

    def __init__(self, *args):
        """Init teradata table."""
        super(self.__class__, self).__init__(*args)

    def get_ddl(self, filter_columns=None):
        """Fetches below column for table ddl.
        | Column Name | Type | Nullable | Format | Max Length |
        Decimal Total Digits | Decimal Fractional Digits """
        if filter_columns is None:
            filter_columns = [0, 1, 2, 4, 5, 6]
        return super(self.__class__, self).get_ddl(filter_columns)

    def set_column_props(self, ddl_rows, increment_column):
        """Set source column names and count."""
        ddl_list = []

        for column_info in ddl_rows:
            if column_info[0] != increment_column:
                column_name = self.convert_special_chars(column_info[0])
                data_type = column_info[1]
                if data_type.lower() == 'd':
                    data_len = column_info[4] + ',' + column_info[5]
                else:
                    data_len = column_info[3]
                ddl_list.append(ColumnDDL(column_name, data_type, data_len))

        self.column_count = len(ddl_list)
        return ddl_list

    def build_ddl_query(self):
        """Build DDL query."""
        query_params = super(self.__class__, self).build_ddl_query()
        query = sql_queries.TD['ddl']
        query = query.format(**query_params)
        return query

    def build_row_count_query(self):
        """Build row count query."""
        query_params = super(self.__class__, self).build_row_count_query()
        query = sql_queries.TD['count']
        query = query.format(**query_params)
        return query

    def build_increment_query(self):
        """Build increment check query."""
        query_params = super(self.__class__, self).build_increment_query()
        query = sql_queries.TD['increment']
        query = query.format(**query_params)
        return query

    def build_row_column_query(self):
        """Build row count query."""
        query_params = super(self.__class__, self).build_row_column_query()
        query = sql_queries.TD['column']
        query = query.format(**query_params)
        return query

    def check_ddl_null(self, column_label, row_data):
        """check db ddl null column for N."""
        index_null = column_label.index('Nullable')
        return not bool(list(column_data for column_data in row_data
                             if column_data[index_null].upper() != 'Y'))


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
    TYPE_MAPPING = {
        SourceTable.STRING: [NUMERIC, MONEY, 'smallmoney', 'real', 'nchar',
                             'ntext', 'nvarchar', 'binary', 'varbinary',
                             'image', 'cursor', 'hierarchyid', 'sql_variant',
                             'table', 'uniqueidentifier', 'xml',
                             'text', VARCHAR],
        SourceTable.CHAR: ['char'], SourceTable.DOUBLE: ['real'],
        SourceTable.TIMESTAMP: DATE_TYPES + ['timestamp'],
        SourceTable.DECIMAL: [DECIMAL], SourceTable.INT: [BIT, INT],
        SourceTable.FLOAT: [FLOAT],
        SourceTable.VARCHAR: [VARCHAR, NUMERIC, SMALLINT, TINYINT,
                              BIGINT, MONEY, FLOAT, DECIMAL, BIT, INT,
                              'smallmoney', 'real', 'nchar', 'ntext',
                              'nvarchar', 'binary', 'varbinary', 'image',
                              'cursor', 'hierarchyid', 'sql_variant', 'table',
                              'uniqueidentifier', 'xml', 'text'] + DATE_TYPES,
        SourceTable.SMALLINT: [SMALLINT], SourceTable.TINYINT: [TINYINT],
        SourceTable.BIGINT: [BIGINT], SourceTable.BOOLEAN: ['char']}

    def __init__(self, *args):
        """Init MSSQL table."""
        super(self.__class__, self).__init__(*args)

    def set_column_props(self, ddl_rows, increment_column):
        """Set source column names and count."""
        ddl_list = []
        for column_info in ddl_rows:
            column_name = column_info[0]
            data_type = column_info[1]

            if column_info[0] != increment_column:

                if data_type.upper() in [self.INT.upper(),
                                         self.DECIMAL.upper(),
                                         self.NUMERIC.upper(),
                                         self.BIGINT.upper(),
                                         self.TINYINT.upper(),
                                         self.SMALLINT.upper(),
                                         self.FLOAT.upper(),
                                         self.MONEY.upper()]:
                    precision = column_info[3]
                    scale = column_info[4]
                    if self.is_null(precision):
                        precision = 0
                    if self.is_null(scale):
                        scale = 0
                    data_len = "{0},{1}".format(precision, scale)
                else:
                    data_len = column_info[2]
                    if self.is_null(data_len):
                        data_len = None
                column_name = self.convert_special_chars(column_name)
                ddl_list.append(ColumnDDL(column_name, data_type, data_len))

            else:
                ddl_list
        self.column_count = len(ddl_list)
        return ddl_list

    def build_ddl_query(self):
        """Build DDL query."""
        query_params = super(self.__class__, self).build_ddl_query()
        query_params['schema_name'] = self.schema
        query = sql_queries.SQLSERVER['ddl']
        query = query.format(**query_params)
        return query

    def build_row_count_query(self):
        """Build row count query."""
        query_params = super(self.__class__, self).build_row_count_query()
        query = sql_queries.SQLSERVER['count']
        query = query.format(**query_params)
        return query

    def build_increment_query(self):
        """Build increment check query."""
        query_params = super(self.__class__, self).build_increment_query()
        query = sql_queries.SQLSERVER['increment']
        query = query.format(**query_params)
        return query

    def build_row_column_query(self):
        """Build row count query."""
        query_params = super(self.__class__, self).build_row_column_query()
        query = sql_queries.SQLSERVER['column']
        query = query.format(**query_params)
        return query

    def check_ddl_null(self, column_label, row_data):
        """check db ddl null column for N."""
        index_null = column_label.index('IS_NULLABLE')
        return not bool(list(column_data for column_data in row_data
                             if column_data[index_null].upper() != 'YES'))


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
    TYPE_MAPPING = {
        SourceTable.STRING: [NUMERIC, 'char', 'text', 'blob', 'tinyblob',
                             'tinytext', 'mediumblob', 'mediumtext',
                             ' longblob', 'longtext', 'enum', 'varchar',
                             'binary', 'varbinary', 'image', 'cursor',
                             'hierarchyid', 'sql_variant', 'table',
                             'uniqueidentifier', 'xml'],
        SourceTable.CHAR: ['char'],
        SourceTable.VARCHAR: [VARCHAR, NUMERIC, INT, BIGINT, SMALLINT,
                              MEDIUMINT, TINYINT, FLOAT, DOUBLE,
                              DECIMAL, NUMERIC, 'char', 'text', 'blob',
                              'tinyblob', 'tinytext', 'mediumblob',
                              'mediumtext', 'longblob', 'longtext', 'enum',
                              'varchar', 'binary', 'varbinary', 'image',
                              'cursor', 'hierarchyid', 'sql_variant',
                              'table', 'uniqueidentifier', 'xml'] + DATE_TYPES,
        SourceTable.TIMESTAMP: DATE_TYPES + ['timestamp'],
        SourceTable.DECIMAL: [DECIMAL], SourceTable.INT: [BIT, INT, MEDIUMINT],
        SourceTable.FLOAT: [FLOAT], SourceTable.DOUBLE: [DOUBLE],
        SourceTable.SMALLINT: [SMALLINT], SourceTable.MEDIUMINT: [MEDIUMINT],
        SourceTable.TINYINT: [TINYINT], SourceTable.BIGINT: [BIGINT],
        SourceTable.BOOLEAN: ['char']}

    def __init__(self, *args):
        """Init MySQL table."""
        super(self.__class__, self).__init__(*args)

    def set_column_props(self, ddl_rows, increment_column):
        """Set source column names and count."""
        ddl_list = []

        for column_info in ddl_rows:
            column_name = column_info[0]
            data_type = column_info[1]

            if column_info[0] != increment_column:

                if data_type.upper() in [self.INT.upper(),
                                         self.DECIMAL.upper(),
                                         self.NUMERIC.upper(),
                                         self.BIGINT.upper(),
                                         self.TINYINT.upper(),
                                         self.SMALLINT.upper(),
                                         self.FLOAT.upper()]:
                    precision = column_info[3]
                    scale = column_info[4]
                    if self.is_null(precision):
                        precision = 0
                    if self.is_null(scale):
                        scale = 0
                    data_len = "{0},{1}".format(precision, scale)
                else:
                    data_len = column_info[2]
                    if self.is_null(data_len):
                        data_len = None
                column_name = self.convert_special_chars(column_name)
                ddl_list.append(ColumnDDL(column_name, data_type, data_len))

            else:
                ddl_list

        self.column_count = len(ddl_list)
        return ddl_list

    def build_ddl_query(self):
        """Build DDL query."""
        query_params = super(self.__class__, self).build_ddl_query()
        query = sql_queries.MYSQL['ddl']
        query = query.format(**query_params)
        return query

    def build_row_count_query(self):
        """Build row count query."""
        query_params = super(self.__class__, self).build_row_count_query()
        query = sql_queries.MYSQL['count']
        query = query.format(**query_params)
        return query

    def build_increment_query(self):
        """Build increment check query."""
        query_params = super(self.__class__, self).build_increment_query()
        query = sql_queries.MYSQL['increment']
        query = query.format(**query_params)
        return query

    def build_row_column_query(self):
        """Build row count query."""
        query_params = super(self.__class__, self).build_row_column_query()
        query = sql_queries.MYSQL['column']
        query = query.format(**query_params)
        return query

    def check_ddl_null(self, column_label, row_data):
        """check db ddl null column for N."""
        index_null = column_label.index('IS_NULLABLE')
        return not bool(list(column_data for column_data in row_data
                             if column_data[index_null].upper() != 'YES'))


class TableValidation(object):
    """Validates the target SQL tables with Hive tables."""

    def __init__(self, source_table, target_table, params):
        """Init."""
        self.params = params
        self.source_table = source_table
        self.target_table = target_table
        self.src_obj = SourceTable(self.source_table, params['host_name'])
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
        msg = "Evaluating Hive: {0} and SQL: {1} tables"
        msg = msg.format(self.source_table, self.target_table)
        logger.info(msg)

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

    def _compare_column_name(self, source_ddls, target_ddls):
        """Compare column names."""
        result = []
        for src_ddl, target_ddl in izip(source_ddls, target_ddls):
            curr_status = False
            # compare column name
            if src_ddl.column_name == target_ddl.column_name:
                curr_status = True
            result.append(curr_status)
        return result

    def _compare_data_type(self, source_ddls, target_ddls):
        """Compare data types."""
        result = []

        for src_ddl, target_ddl in izip(source_ddls, target_ddls):
            curr_status = False
            if target_ddl.data_type in self.target_obj.TYPE_MAPPING[
                    src_ddl.data_type]:
                curr_status = True
            result.append(curr_status)

        return result

    def _compare_data_len(self, source_ddls, target_ddls):
        """Compare data length."""
        result = []
        for src_ddl, target_ddl in izip(source_ddls, target_ddls):
            curr_status = False
            if target_ddl.data_type in SourceTable.NON_LEN_TYPES:
                # hive does not have length property for certain fields
                curr_status = True
            else:
                curr_status = True if src_ddl.data_len <= \
                    target_ddl.data_len else False
                if src_ddl.precision and target_ddl.precision:
                    curr_status = True if src_ddl.precision <= \
                        target_ddl.precision else False
                if src_ddl.scale and target_ddl.scale:
                    curr_status = True if src_ddl.scale <= \
                        target_ddl.scale else False
            result.append(curr_status)
        return result

    def ddl_matches(self, source_ddls, target_ddls):
        """Compare column name, data type, data length."""
        logger.info("DDL validation: comparing column name, "
                    "data type and data length")
        status = False
        mismatched_ddls = []
        matched_ddls = []
        match_count = 0
        total_count = len(source_ddls)
        name_result = self._compare_column_name(source_ddls, target_ddls)
        data_type_result = self._compare_data_type(source_ddls, target_ddls)
        data_len_result = self._compare_data_len(source_ddls, target_ddls)

        for index, name_r, type_r, len_r in izip(range(total_count),
                                                 name_result, data_type_result,
                                                 data_len_result):
            if name_r and type_r and len_r:
                match_count += 1
                matched_ddls.append(((name_r, type_r, len_r),
                                     source_ddls[index],
                                     target_ddls[index]))
            else:
                mismatched_ddls.append(((name_r, type_r, len_r),
                                        source_ddls[index],
                                        target_ddls[index]))
        mismatch_count = total_count - match_count
        logger.info('Match: {0}, Mismatch: {1}'.format(match_count,
                                                       mismatch_count))

        if mismatch_count > 0:
            for info in mismatched_ddls:
                _err_msg = "\nMismatched ddls: Name, Type, Len:{0}\n{1}\n{2}"
                _err_msg = _err_msg.format(info[0], info[1], info[2])
                logger.error(_err_msg)
                logger.error('-' * 50)
            err_msg = "FAILED. DDL comparision. Matched: {0}, Mismatched: {1}"
            err_msg = err_msg.format(match_count, mismatch_count)
            logger.error(err_msg)
        else:
            logger.info('DDL matched!')
            for info in matched_ddls:
                _err_msg = "\nMatched ddls: Name, Type, Len:{0}\n{1}\n{2}"
                _err_msg = _err_msg.format(info[0], info[1], info[2])
                logger.error(_err_msg)
                logger.error('-' * 50)
            info_msg = "PASSED. DDL comparision. Matched: {0}, Mismatched: {1}"
            info_msg = info_msg.format(match_count, mismatch_count)
            logger.info(info_msg)
            status = True
        return status

    def ddl_column_matches(self, column_labels, row_data):
        if self.target_obj.db_vendor == DB2:
            column_name_list = [column_data[0] for column_data in row_data]
            column_name_list.sort()
            column_labels.sort()
            return bool(column_name_list == column_labels)
        return True

    def start_data_sampling(self):
        """Start qa data sampling"""
        if not self.src_obj:
            return True
        logger.info('Starting export pre QA...')
        qa_stages = {}

        target_rowcount = self.target_obj.get_target_row_count()
        if target_rowcount >= 1:
            print ("ERROR----> Target table is not empty. Please contact DBA"
                   " to truncate the target table")
            sys.exit(1)

        column = self.target_obj.get_target_increment_column()
        column_labels, row_data = self.target_obj.get_ddl()
        table_column_labels, table_row_data = self.target_obj.get_ddl_column()
        source_ddls = self.src_obj.get_ddl()
        if column:
            target_ddls = self.target_obj.get_ddl_list(row_data,
                                                       increment_column=column)
        else:
            target_ddls = self.target_obj.get_ddl_list(row_data)

        self.ddl_bool = self.ddl_matches(source_ddls, target_ddls)
        qa_stages['ddl'] = self.ddl_bool
        self.ddl_null_check = self.target_obj.check_ddl_null(column_labels,
                                                             row_data)
        if not self.ddl_null_check:
            logger.error("All columns in table have to accept null values")
        qa_stages['ddl_null_check'] = self.ddl_null_check

        self.ddl_column_bool = self.ddl_column_matches(table_column_labels,
                                                       row_data)
        if not self.ddl_column_bool:
            logger.error("DD2 column names are not in sync with DB2 catalog")
        qa_stages['ddl_column'] = self.ddl_column_bool

        logger.qa_status(qa_stages)
        return [self.ddl_bool, self.ddl_null_check, self.ddl_column_bool]


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
        'target_schema': sys.argv[11],
        'qa_exp_results_dir': sys.argv[13]
    }
    DEBUG = True if sys.argv[12] == 'true' else False

    source_table = args['source_database_name'] + '.' + \
        args['source_table_name']
    logger = LogHandler(source_table,
                        args['host_name'],
                        args['qa_exp_results_dir'])
    try:

        target_table = args['database'] + '.' + args['target_table']
        val_obj = TableValidation(source_table, target_table, args)
        bool_status_list = val_obj.start_data_sampling()
        logger.insert_hive()
        exit_code = 0 if bool_status_list[0] else 1
        if 'db2' in args['jdbc_url']:
            exit_code = 0 if (bool_status_list[0] and
                              bool_status_list[1] and
                              bool_status_list[2]) else 1
    except Exception as e:
        exit_code = 1
        logger.error(traceback.format_exc())
        logger.insert_hive()

    ImpalaConnect.close_conn()
    sys.exit(exit_code)

if __name__ == '__main__':
    if len(sys.argv) != 14:
        print "ERROR----> command line args are not proper"
        print sys.argv
        sys.exit(1)
    main()
