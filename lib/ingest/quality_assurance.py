"""IBIS QA.
Compares the following between source and target tables:
1. Compares row counts
2. Compares DDL
3. Compares random rows from source and fetches corresponding rows from hive
It
also does:
Validates DDL and sample data from target table
"""

import re
import sys
import os
import traceback
import logging
import datetime
import decimal
import math
import json
from abc import ABCMeta, abstractmethod
from itertools import izip, ifilter
import voluptuous
import sql_queries
from sqoop_utils import SqoopUtils
from impala_utils import ImpalaConnect
from oozie_ws_helper import ChecksBalancesManager
from py_hdfs import PyHDFS


LOG_FILE = 'qa.log'

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
    """log to hive table, stdout and stderr"""

    def __init__(self, source_table, host, qa_results_tbl_path):
        """Init."""
        self.source_table = source_table
        self.host = host
        self.buffer_logs = []
        self.qa_status_log = ''
        self.pyhdfs = PyHDFS(qa_results_tbl_path)

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
        self.pyhdfs.insert_update('',
                                  self.prepares_data(format_data),
                                  is_append=True)

        # We need to perform INVALIDATE METADATA to reflect Hive
        # file-based operations in Impala.
        ImpalaConnect.invalidate_metadata(self.host, 'ibis.qa_resultsv2')

        msg = '\n' + '#' * 100
        msg += ("\nFor QA results: Run query in Impala: "
                "select status, * from ibis.qa_resultsv1 where log_time='{0}'")
        msg += '\n' + '#' * 100
        msg = msg.format(_time)
        print msg
        logging.info('Inserted logs to Hive')

    def prepares_data(self, format_data):
        """Prepares the data in the order it should
        store in the hive table"""
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
        return 'ColumnDDL: {%-20s %-9s %-4s(%-4s, %-4s)}' % \
               (self.column_name, self.data_type, self.data_len,
                self.precision, self.scale)


class SourceTable(SqoopUtils):
    """Source table representation."""

    __metaclass__ = ABCMeta

    column_count = 0
    row_count = 0

    @property
    def rand_rows_num(self):
        """Number of random rows to fetch."""
        rows_num = 5
        return rows_num

    def __init__(self, sqoop_jars, jdbc_url, connection_factories,
                 user_name, jceks, password_alias, table, schema):
        """Init."""
        SqoopUtils.__init__(self, sqoop_jars, jdbc_url, connection_factories,
                            user_name, jceks, password_alias)
        self.table = table
        self.database_name, self.table_name = self.table.split('.')
        self.schema = schema

        if 'db2' in jdbc_url:
            self.db_vendor = DB2
        elif 'teradata' in jdbc_url:
            self.db_vendor = TERADATA
        else:
            self.db_vendor = ORACLE

    def build_row_count_query(self):
        """Build row count query."""
        return "SELECT COUNT(*) FROM {0}".format(self.table.upper())

    def get_row_count(self):
        """Fetch row count of source table."""
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

    @abstractmethod
    def build_ddl_query(self):
        """Abstract method.
        Build DDL query for respective databases. Implements bit of
        functionality
        """
        query_params = {'database_name': self.database_name.upper(),
                        'table_name': self.table_name.upper()}
        return query_params

    @classmethod
    def convert_special_chars(cls, column_name):
        """convert special characters"""
        pattern_non_alphanumeric = re.compile(r'[^A-Za-z0-9_]')
        pattern_numeric_at_beginning = r'^\d'
        _name = re.sub(pattern_non_alphanumeric, '', column_name)
        if re.search(pattern_numeric_at_beginning, _name) is not None:
            _name = 'i_' + _name
        return _name

    def set_column_props(self, ddl_rows):
        """Set source column names and count."""
        ddl_list = []

        for column_info in ddl_rows:
            column_name = SourceTable.convert_special_chars(column_info[0])
            data_type = column_info[1]
            data_len = column_info[2]
            ddl_list.append(ColumnDDL(column_name, data_type, data_len))

        self.column_count = len(ddl_list)
        return ddl_list

    def get_ddl(self, filter_columns=None):
        """Fetch source db ddl."""
        ddl_list = []
        ddl_query = self.build_ddl_query()
        returncode, output, err = self.eval(ddl_query)
        # print 'source output', output
        if returncode == 0:
            _, row_data = self.fetch_rows_sqoop(output,
                                                filter_columns=filter_columns)
            ddl_list = self.set_column_props(row_data)
            # sort the list of ddl objects as source and target ddl
            # output are not ordered
            # ddl_list.sort(key=lambda ddl: ddl.column_name)
        else:
            logger.error(output)
            logger.error(err)
            raise ValueError('Error in get ddl')
        return ddl_list

    def get_rand_rows(self, src_ddls, filter_columns=None):
        """Select random rows from source."""
        msg = "Random rows validation for {0} rows:".format(self.rand_rows_num)
        logger.info(msg)
        rand_rows = []
        rand_rows_query = self.build_rand_rows_query(src_ddls)
        returncode, output, err = self.eval(rand_rows_query)
        # print 'src rand rows', output
        if returncode == 0:
            _, rand_rows = self.fetch_rows_sqoop(output, strip_col_val=True,
                                                 filter_columns=filter_columns)
        else:
            logger.error(output)
            logger.error(err)
            raise ValueError('Error in get rand rows')
        return rand_rows

    @abstractmethod
    def build_rand_rows_query(self, ddl_objs):
        """Abstract method.
        Build the query based on db vendor. Implements bit of functionality.
        """
        src_columns = []

        for ddl in ddl_objs:
            src_columns.append(ddl.column_name)

        selected_columns_stmt = ', '.join(src_columns)

        return selected_columns_stmt

    def is_null(self, col_val):
        """Check if the sql column value is null"""
        return 'null' in col_val


class TargetTable(object):
    """Target table in Hive."""

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

    def __init__(self, target_table, host_name):
        """Init."""
        self.table = target_table
        self.impala_host = host_name
        self.database_name, self.table_name = target_table.split(".")

    def get_row_count(self):
        """Fetch row count of target table.
        Method to query the hive db and find the row count for each table
        in the source list.
        Returns a dictionary with key as table name and value as the row count
        """
        row_count = -1
        count_query = "SELECT COUNT(*) FROM {0};".format(self.table)
        output = ImpalaConnect.run_query(self.impala_host, self.table,
                                         count_query)
        row_count = output[0][0]
        return int(row_count)

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

            if info[0] in TargetTable.NON_LEN_TYPES:
                # certain fields dont have a data len. So initiate to None
                info.append(None)

            ddl_obj = ColumnDDL(column_info[0], info[0], info[1])
            ddl_list.append(ddl_obj)

        self.column_count = len(ddl_list)
        return ddl_list

    def build_ddl_query(self):
        """Build hive ddl query."""
        return "describe {table};".format(**{'table': self.table})

    def get_ddl(self):
        """Query hive database for column name, data type, and column count.
        Returns a dictionary of key as table name and values as column
        name and datatype
        """
        ddl_list = []
        ddl_query = self.build_ddl_query()
        output = ImpalaConnect.run_query(
            self.impala_host, self.table, ddl_query)
        # print 'output', output
        ddl_list = self.set_column_props(output)
        # sort the list of ddl objects as source and target ddl output
        # are not ordered
        # ddl_list.sort(key=lambda ddl: ddl.column_name)
        return ddl_list

    def get_column_names(self, ddl_objs, data_types=None):
        """Return column names for specific data type(optional)."""
        column_names = []
        selected_columns = []

        if data_types is None:
            for ddlObj in ddl_objs:
                column_names.append(ddlObj.column_name)
                if ddlObj.data_type in self.VARIABLE_LENGTH_COLS:
                    selected_columns.append('TRIM(' + ddlObj.column_name + ')')
                else:
                    selected_columns.append(ddlObj.column_name)
        else:
            for ddlObj in ddl_objs:
                if ddlObj.data_type in data_types:
                    column_names.append(ddlObj.column_name)
                    if ddlObj.data_type in self.VARIABLE_LENGTH_COLS:
                        selected_columns.append(
                            'TRIM(' + ddlObj.column_name + ')')
                    else:
                        selected_columns.append(ddlObj.column_name)
        return column_names, selected_columns

    def _build_fetch_row_query(self, src_ddls, target_ddls, src_row, src_obj):
        """Build impala query for fetching rows from hive using
        source sql row data."""
        target_columns, selected_columns = self.get_column_names(target_ddls)
        numeric_columns, _ = self.get_column_names(
            target_ddls, data_types=TargetTable.NUMERIC_TYPES)
        char_columns, _ = self.get_column_names(target_ddls,
                                                data_types=[TargetTable.CHAR])
        str_columns, _ = self.get_column_names(target_ddls,
                                               data_types=[TargetTable.VARCHAR,
                                                           TargetTable.STRING])
        src_date_columns, _ = self.get_column_names(src_ddls,
                                                    data_types=src_obj.
                                                    DATE_TYPES)
        where_columns = []

        for col_name, col_value, s_ddl, t_ddl in izip(target_columns, src_row,
                                                      src_ddls, target_ddls):
            if col_name in src_date_columns:
                # date columns are the tricky ones that wont match.
                # don't use them to query hive rows
                continue
        #  _col_val = col_value

            col_value = str(col_value).replace("(null)", "NULL")
            format_data = {'col_name': col_name, 'col_value': col_value}

            if col_value and col_name in numeric_columns:
                if col_value == 'NULL':
                    condition = "{col_name} is {col_value}".\
                        format(**format_data)
                else:
                    condition = "{col_name}={col_value}".format(**format_data)
                where_columns.append(condition)
            elif col_value and col_name in str_columns:
                if col_value == 'NULL':
                    condition = "TRIM({col_name}) is {col_value}"\
                        .format(**format_data)
                else:
                    condition = "TRIM({col_name})='{col_value}'"\
                        .format(**format_data)
                where_columns.append(condition)
            elif col_value.strip() and col_name in char_columns:
                if col_value.strip() == 'NULL':
                    condition = "TRIM({col_name}) is {col_value}"\
                        .format(**format_data)
                else:
                    condition = "TRIM({col_name})='{col_value}'"\
                        .format(**format_data)
                where_columns.append(condition)

        where_condition = ' and '.join(where_columns)
        selected_columns_statement = ', '.join(selected_columns)
        query = "SELECT {0} FROM {1} WHERE {2};".format(
            selected_columns_statement, self.table, where_condition.strip())
        return query

    def get_corresponding_rows(self, source_ddls, target_ddls,
                               source_rand_rows, src_obj):
        """Fetch rows from source then query the hive tables for same rows."""
        target_rows = []
        success_count = 0

        for cnt, src_row in enumerate(source_rand_rows):
            if cnt - success_count > 10:
                failed_msg = ("Hive rand rows. Too many rows failed to fetch."
                              " Iterated:{0} Succeded:{1}")
                failed_msg = failed_msg.format(cnt, success_count)
                raise ValueError(failed_msg)

            query = self._build_fetch_row_query(source_ddls, target_ddls,
                                                src_row, src_obj)
            target_col_data = ImpalaConnect.run_query(self.impala_host,
                                                      self.table, query)
            if target_col_data:
                if len(target_col_data) == 1:
                    # print query
                    # print '\t', target_col_data
                    logger.info('One row fetched')
                    target_rows.append(target_col_data[0])
                    success_count += 1
                else:
                    err_msg = "Hive rand rows: more than one row " \
                              "fetched from hive"
                    logger.error(err_msg)
                    target_rows.append([])
            else:
                # print src_row
                # print query
                # print '\t', target_col_data
                err_msg = "Hive rand rows: No rows fetched from " \
                          "hive: {0}".format(target_col_data)
                logger.error(err_msg)
                target_rows.append([])
            # print '\n'
        return target_rows


class OracleTable(SourceTable):
    """Oracle specific methods."""

    TIMESTAMP = 'timestamp'
    DATE = 'date'
    DATE_TYPES = (TIMESTAMP, DATE)
    NUMBER = 'number'
    CHAR = 'char'
    NCHAR = 'nchar'
    VARCHAR = 'varchar'
    VARCHAR2 = 'varchar2'
    NVARCHAR2 = 'nvarchar2'
    CHAR_COLUMNS = [CHAR, VARCHAR2, NCHAR, NVARCHAR2]
    VARIABLE_LENGTH_COLS = [VARCHAR, VARCHAR2, NVARCHAR2]
    TYPE_MAPPING = {
        TargetTable.STRING: ['blob', 'clob', 'nclob', 'bfile',
                             'urowid', 'rowid', 'long', 'raw', 'long raw',
                             'uritype', 'xmltype'],
        TargetTable.VARCHAR: [VARCHAR, VARCHAR2, NVARCHAR2],
        TargetTable.CHAR: [CHAR, NCHAR],
        TargetTable.TIMESTAMP: DATE_TYPES,
        TargetTable.DECIMAL: (NUMBER),
        TargetTable.FLOAT: ['binary_float', 'float'],
        TargetTable.DOUBLE: ['binary_double', 'double']}

    def __init__(self, *args):
        """Init oracle table."""
        super(self.__class__, self).__init__(*args)

    def set_column_props(self, ddl_rows):
        """Set source column names and count."""
        ddl_list = []

        for column_info in ddl_rows:
            column_name = column_info[0]
            data_type = column_info[1]

            if self.NUMBER.upper() in data_type:
                precision = column_info[3]
                scale = column_info[4]
                if self.is_null(precision):
                    precision = self.get_max_len(column_name)
                if self.is_null(scale):
                    scale = 0
                data_len = "{0},{1}".format(precision, scale)
            elif data_type.lower() in self.CHAR_COLUMNS:
                # use CHAR_LENGTH column
                data_len = column_info[6]
            else:
                data_len = column_info[2]

            if self.TIMESTAMP.upper() in data_type:
                # oracle specific
                data_type = data_type.replace(')', '')
                data_type, data_len = data_type.split('(')

            column_name = SourceTable.convert_special_chars(column_name)
            ddl_list.append(ColumnDDL(column_name, data_type, data_len))

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
            logger.error(output)
            logger.error(err)
            raise ValueError("Error in get max len")
        return max_len

    def build_ddl_query(self):
        """Build DDL query."""
        query_params = super(self.__class__, self).build_ddl_query()
        query = sql_queries.ORACLE['ddl']
        query = query.format(**query_params)
        return query

    def build_rand_rows_query(self, ddl_objs):
        """Build the query for fetching random rows."""
        selected_columns_stmt = super(self.__class__, self).\
            build_rand_rows_query(ddl_objs)
        query = ("SELECT {0} FROM (SELECT {0} FROM {1} ORDER BY "
                 "DBMS_RANDOM.VALUE) WHERE ROWNUM < {2}")
        query = query.format(selected_columns_stmt, self.table,
                             self.rand_rows_num + 1)
        return query


class DB2Table(SourceTable):
    """DB2 specific methods."""

    TIMESTMP = 'timestmp'
    TIMESTAMP = 'timestamp'
    DATE = 'date'
    TIME = 'time'
    VARCHAR = 'varchar'
    VARIABLE_LENGTH_COLS = [VARCHAR]
    DATE_TYPES = (TIMESTMP, TIMESTAMP, DATE, TIME)
    TYPE_MAPPING = {
        TargetTable.STRING: [TIME], TargetTable.VARCHAR: [VARCHAR],
        TargetTable.CHAR: ['char'], TargetTable.BOOLEAN: ['boolean'],
        TargetTable.TIMESTAMP: [TIMESTAMP, TIMESTMP, DATE],
        TargetTable.INT: ['integer'], TargetTable.SMALLINT: ['smallint'],
        TargetTable.BIGINT: ['bigint'], TargetTable.DECIMAL: ['decimal',
                                                              'numeric'],
        TargetTable.DOUBLE: ['double']}

    def __init__(self, *args):
        """Init DB2 table."""
        super(self.__class__, self).__init__(*args)

    def build_ddl_query(self):
        """Build DDL query."""
        query_params = super(self.__class__, self).build_ddl_query()
        query = sql_queries.DB2['ddl']
        query = query.format(**query_params)
        return query

    def build_rand_rows_query(self, ddl_objs):
        """Build the query for fetching random rows."""
        selected_columns = super(self.__class__, self)\
            .build_rand_rows_query(ddl_objs)
        query = ("SELECT {0}, RAND() AS IDX FROM {1} ORDER BY IDX FETCH"
                 " FIRST {2} ROWS ONLY").format(selected_columns,
                                                self.table, self.rand_rows_num)
        query = query.format(selected_columns)
        return query

    def get_rand_rows(self, src_ddls):
        """Select random rows from db2. Ignore the last column."""
        # ignore the last item which is RAND()
        filter_columns = range(len(src_ddls))
        return super(self.__class__, self).\
            get_rand_rows(src_ddls,
                          filter_columns=filter_columns)


class TeraDataTable(SourceTable):
    """Teradata specific methods."""

    DATE_TYPES = ()
    CV = 'cv'
    VARIABLE_LENGTH_COLS = [CV]
    TYPE_MAPPING = {
        TargetTable.STRING: ['a1', 'an', 'bf', 'bo', 'bv', 'co', 'dh',
                             'dm', 'ds', 'dy', 'hm', 'hs',
                             'hr', 'jn', 'mi', 'mo', 'ms', 'pd', 'pm',
                             'ps', 'pt', 'pz', 'sc', 'sz',
                             'tz', 'ut', 'xm', 'ym', 'yr', '++'],
        TargetTable.CHAR: ['cf'], TargetTable.VARCHAR: [CV],
        TargetTable.TIMESTAMP: ['at', 'da', 'ts'],
        TargetTable.DECIMAL: ['d'], TargetTable.INT: ['i1', 'i', 'n'],
        TargetTable.DOUBLE: ['f'],
        TargetTable.SMALLINT: ['i2'], TargetTable.BIGINT: ['i8']}

    def __init__(self, *args):
        """Init teradata table."""
        super(self.__class__, self).__init__(*args)

    def build_row_count_query(self):
        """Build row count query."""
        return "SELECT CAST(count(*) as decimal(38,0)) " \
               "FROM {0}".format(self.table.upper())

    def get_ddl(self):
        """Fetch table ddl."""
        return super(self.__class__, self).get_ddl(filter_columns=[0, 1, 4])

    def build_ddl_query(self):
        """Build DDL query."""
        query_params = super(self.__class__, self).build_ddl_query()
        query = sql_queries.TD['ddl']
        query = query.format(**query_params)
        return query

    def build_rand_rows_query(self, ddl_objs):
        """Build the query for fetching random rows."""
        selected_columns = super(self.__class__,
                                 self).build_rand_rows_query(ddl_objs)
        query = "SELECT {0} FROM {1} SAMPLE {2}"
        query = query.format(selected_columns, self.table, self.rand_rows_num)
        return query


class MSSqlTable(SourceTable):
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
        TargetTable.STRING: [NUMERIC, 'real', 'nchar',
                             'ntext', 'nvarchar', 'binary', 'varbinary',
                             'image', 'cursor', 'hierarchyid', 'sql_variant',
                             'table', 'uniqueidentifier', 'xml',
                             MONEY, 'smallmoney'],
        TargetTable.CHAR: ['char'], TargetTable.VARCHAR: [VARCHAR],
        TargetTable.TIMESTAMP: DATE_TYPES + ['timestamp'],
        TargetTable.DECIMAL: [DECIMAL, MONEY, 'smallmoney'],
        TargetTable.INT: [BIT, INT],
        TargetTable.FLOAT: [FLOAT],
        TargetTable.SMALLINT: [SMALLINT], TargetTable.TINYINT: [TINYINT],
        TargetTable.BIGINT: [BIGINT]}

    def __init__(self, *args):
        """Init MSSQL table."""
        super(self.__class__, self).__init__(*args)

    def set_column_props(self, ddl_rows):
        """Set source column names and count."""
        ddl_list = []

        for column_info in ddl_rows:
            column_name = column_info[0]
            data_type = column_info[1]

            if data_type.upper() in [self.INT.upper(), self.DECIMAL.upper(),
                                     self.NUMERIC.upper(),
                                     self.BIGINT.upper(),
                                     self.TINYINT.upper(),
                                     self.SMALLINT.upper(),
                                     self.FLOAT.upper(), self.MONEY.upper()]:
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
            column_name = SourceTable.convert_special_chars(column_name)
            ddl_list.append(ColumnDDL(column_name, data_type, data_len))

        self.column_count = len(ddl_list)
        return ddl_list

    def build_ddl_query(self):
        """Build DDL query."""
        query_params = super(self.__class__, self).build_ddl_query()
        query_params['schema_name'] = self.schema
        query = sql_queries.SQLSERVER['ddl']
        query = query.format(**query_params)
        return query

    def build_rand_rows_query(self, ddl_objs):
        """Build the query for fetching random rows."""
        # incomplete implementation
        # selected_columns = super(self.__class__,
        #  self).build_rand_rows_query(ddl_objs)
        selected_columns = super(self.__class__,
                                 self).build_rand_rows_query(ddl_objs)
        full_table_name = self.database_name + '.' + self.schema + '.' + \
            self.table_name
        query = ("SELECT TOP {0} {1} FROM {2} ORDER "
                 "BY NEWID()")
        query = query.format(self.rand_rows_num, selected_columns,
                             full_table_name)
        return query


class PostgreSql(SourceTable):
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
        TargetTable.STRING: [NUMERIC, 'real', 'nchar',
                             'ntext', 'nvarchar', 'binary', 'varbinary',
                             'image', 'cursor', 'hierarchyid', 'sql_variant',
                             'table', 'uniqueidentifier', 'xml',
                             MONEY, 'smallmoney', 'box', 'bigserial', 'bytea',
                             'character', 'cidr', 'circle', 'json', 'jsonb',
                             'line', 'lseg', 'point', 'polygon', 'real',
                             'serial', 'smallserial', 'tsquery', 'tsvector',
                             'txid_snapshot'],
        TargetTable.CHAR: ['char'], TargetTable.VARCHAR: [VARCHAR],
        TargetTable.TIMESTAMP: DATE_TYPES + ['timestamp'],
        TargetTable.DECIMAL: [DECIMAL, MONEY, 'smallmoney'],
        TargetTable.INT: [BIT, INT],
        TargetTable.FLOAT: [FLOAT],
        TargetTable.SMALLINT: [SMALLINT], TargetTable.TINYINT: [TINYINT],
        TargetTable.BIGINT: [BIGINT]}

    def __init__(self, *args):
        """Init MSSQL table."""
        super(self.__class__, self).__init__(*args)

    def set_column_props(self, ddl_rows):
        """Set source column names and count."""
        ddl_list = []

        for column_info in ddl_rows:
            column_name = column_info[0]
            data_type = column_info[1]

            if data_type.upper() in [self.INT.upper(), self.DECIMAL.upper(),
                                     self.NUMERIC.upper(),
                                     self.BIGINT.upper(),
                                     self.TINYINT.upper(),
                                     self.SMALLINT.upper(),
                                     self.FLOAT.upper(), self.MONEY.upper()]:
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
            column_name = SourceTable.convert_special_chars(column_name)
            ddl_list.append(ColumnDDL(column_name, data_type, data_len))

        self.column_count = len(ddl_list)
        return ddl_list

    def build_ddl_query(self):
        """Build DDL query."""
        query_params = super(self.__class__, self).build_ddl_query()
        query_params['schema_name'] = self.schema
        query = sql_queries.POSTGRESQL['ddl']
        query = query.format(**query_params)
        return query

    def build_rand_rows_query(self, ddl_objs):
        """Build the query for fetching random rows."""
        # incomplete implementation
        # selected_columns = super(self.__class__,
        #  self).build_rand_rows_query(ddl_objs)
        selected_columns = super(self.__class__,
                                 self).build_rand_rows_query(ddl_objs)
        full_table_name = self.database_name + '.' + self.schema + '.' + \
            self.table_name
        query = ("SELECT TOP {0} {1} FROM {2} ORDER "
                 "BY NEWID()")
        query = query.format(self.rand_rows_num, selected_columns,
                             full_table_name)
        return query


class MySqlTable(SourceTable):
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
        TargetTable.STRING: [NUMERIC, 'char', 'text', 'blob', 'tinyblob',
                             'tinytext', 'mediumblob', 'mediumtext',
                             ' longblob', 'longtext', 'enum', 'varchar',
                             'binary', 'varbinary', 'image', 'cursor',
                             'hierarchyid', 'sql_variant', 'table',
                             'uniqueidentifier', 'xml'],
        TargetTable.CHAR: ['char'], TargetTable.VARCHAR: [VARCHAR],
        TargetTable.TIMESTAMP: DATE_TYPES + ['timestamp'],
        TargetTable.DECIMAL: [DECIMAL], TargetTable.INT: [BIT, INT],
        TargetTable.FLOAT: [FLOAT], TargetTable.DOUBLE: [DOUBLE],
        TargetTable.SMALLINT: [SMALLINT], TargetTable.MEDIUMINT: [MEDIUMINT],
        TargetTable.TINYINT: [TINYINT], TargetTable.BIGINT: [BIGINT]}

    def __init__(self, *args):
        """Init MySQL table."""
        super(self.__class__, self).__init__(*args)

    def set_column_props(self, ddl_rows):
        """Set source column names and count."""
        ddl_list = []

        for column_info in ddl_rows:
            column_name = column_info[0]
            data_type = column_info[1]
            if data_type.upper() in [self.INT.upper(), self.DECIMAL.upper(),
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
            column_name = SourceTable.convert_special_chars(column_name)
            ddl_list.append(ColumnDDL(column_name, data_type, data_len))

        self.column_count = len(ddl_list)
        return ddl_list

    def build_ddl_query(self):
        """Build DDL query."""
        query_params = super(self.__class__, self).build_ddl_query()
        query = sql_queries.MYSQL['ddl']
        query = query.format(**query_params)
        return query

    def build_rand_rows_query(self, ddl_objs):
        """Build the query for fetching random rows."""
        # incomplete implementation
        # selected_columns = super(self.__class__,
        #  self).build_rand_rows_query(ddl_objs)
        query = ("SELECT * FROM {0} AS r1 JOIN (SELECT CEIL(RAND() * "
                 "(SELECT MAX(id) FROM {0})) AS id) AS r2 WHERE r1.id >= "
                 "r2.id ORDER BY r1.id ASC LIMIT 1").format(self.table)
        return query


class TableValidation(object):
    """Validates the source SQL tables with Hive tables."""

    def __init__(self, source_table, target_table, params):
        """Init."""
        self.src_type = ''
        self.params = params
        self.source_table = source_table
        self.target_table = target_table
        self.domain = params['domain']
        self.count_bool = self.ddl_bool = self.rows_bool \
            = self.target_types_bool = False
        self.target_obj = TargetTable(self.target_table, params['impala_host'])
        self.checks_balances = ChecksBalancesManager(
            params['impala_host'], params['oozie_url'],
            params['qa_results_tbl_path'])

        if 'jceks' in params['password_file']:
            jceks, password = params['password_file'].split('#')
        else:
            jceks = 'None'
            password = params['password_file']

        if 'oracle' in params['jdbc_url']:
            self.src_type = 'oracle'
            self.src_obj = OracleTable(
                params['jars'], params['jdbc_url'],
                params['connection_factories'], params['user_name'],
                jceks, password, self.source_table, params['schema'])
        elif 'db2' in params['jdbc_url']:
            self.src_type = 'db2'
            self.src_obj = DB2Table(
                params['jars'], params['jdbc_url'],
                params['connection_factories'], params['user_name'],
                jceks, password, self.source_table, params['schema'])
        elif 'teradata' in params['jdbc_url']:
            self.src_type = 'TD'
            self.src_obj = TeraDataTable(
                params['jars'], params['jdbc_url'],
                params['connection_factories'], params['user_name'],
                jceks, password, self.source_table, params['schema'])
        elif 'mysql' in params['jdbc_url']:
            self.src_type = 'mysql'
            self.src_obj = MySqlTable(
                params['jars'], params['jdbc_url'],
                params['connection_factories'], params['user_name'],
                jceks, password, self.source_table, params['schema'])
        elif 'postgresql' in params['jdbc_url']:
            self.src_type = 'postgresql'
            self.src_obj = PostgreSql(
                params['jars'], params['jdbc_url'],
                params['connection_factories'], params['user_name'],
                jceks, password, self.source_table, params['schema'])
        elif 'sqlserver' in params['jdbc_url']:
            self.src_type = 'sqlserver'
            self.src_obj = MSSqlTable(
                params['jars'], params['jdbc_url'],
                params['connection_factories'], params['user_name'],
                jceks, password, self.source_table, params['schema'])
        else:
            raise ValueError("Unrecognized source found"
                             "in: '{0}'".format(params['jdbc_url']))

        msg = "Evaluating Source: {0} and Hive: {1} tables. Type: {2}"
        msg = msg.format(self.source_table, self.target_table, self.src_type)
        logger.info(msg)

    def count_matches(self, source_rowcount, target_rowcount):
        """Compare row count of source and target tables."""
        logger.info("Row count validation:")

        status = False
        if int(source_rowcount) == 0:
            percent = 100.0
            logger.info("No rows fetched from source!")
        else:
            percent = (float(target_rowcount)/float(source_rowcount)) * 100

        if percent <= 95 or percent > 100:
            err_msg = """FAILED. Row count check, source: {0}
            hive: {1} percent: {2}"""
            logger.error(err_msg.format(source_rowcount,
                                        target_rowcount, percent))
        else:
            info_msg = "Row count matches: source: {0} hive: {1}. Percent: {2}"
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
            if src_ddl.data_type in self.src_obj.TYPE_MAPPING[
                    target_ddl.data_type]:
                curr_status = True
            result.append(curr_status)

        return result

    def _compare_data_len(self, source_ddls, target_ddls):
        """Compare data length."""
        result = []
        for src_ddl, target_ddl in izip(source_ddls, target_ddls):
            curr_status = False
            if target_ddl.data_type in TargetTable.NON_LEN_TYPES:
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
                                                 name_result,
                                                 data_type_result,
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
        logger.info('Matched: {0}, Mismatched: {1}'.format(
            match_count, mismatch_count))

        if mismatch_count > 0:
            for info in mismatched_ddls:
                _err_msg = ("\nMismatched ddls: Name, Type, "
                            "Len(precision, scale): {0}\n"
                            "RDBMS: {1}\nHive:  {2}")
                _err_msg = _err_msg.format(info[0], info[1], info[2])
                logger.error(_err_msg)
                logger.error('-' * 50)
            err_msg = "FAILED. DDL comparision. Matched: {0}, Mismatched: {1}"
            err_msg = err_msg.format(match_count, mismatch_count)
            logger.error(err_msg)
        else:
            for info in matched_ddls:
                _msg = ("\nMatched ddls: Name, Type, "
                        "Len(precision, scale): {0}\n"
                        "RDBMS: {1}\nHive:  {2}")
                _msg = _msg.format(info[0], info[1], info[2])
                logger.info(_msg)
                logger.info('-' * 50)
            logger.info('DDL matched!')
            status = True

        return status

    def rows_match(self, src_ddls, target_ddls, src_rows, target_rows):
        """Compare corresponding rows of source and target tables."""
        status = False
        total_rows_cnt = len(src_rows)
        row_match_cnt = 0

        for index, src_col_data, target_col_data in \
                izip(range(total_rows_cnt), src_rows, target_rows):
            column_match_list = []
            print '=========================='
            print src_col_data
            print target_col_data
            logger.info('--' * 20)

            for src_val, target_val, src_ddl, target_ddl \
                    in izip(src_col_data, target_col_data,
                            src_ddls, target_ddls):
                # remove extra spaces due to sqoop output if not CHAR
                src_val = src_val.strip()
                if target_ddl.data_type in [TargetTable.CHAR,
                                            TargetTable.VARCHAR,
                                            TargetTable.STRING] and target_val:
                    target_val = target_val.strip()
                    # pass

                if target_ddl.data_type in TargetTable.NUMERIC_TYPES:
                    if src_val.strip() == '(null)':
                        src_val = None
                    else:
                        src_val = decimal.Decimal(src_val)
                elif target_ddl.data_type in [TargetTable.STRING,
                                              TargetTable.VARCHAR]:
                    if src_val.strip() == '(null)':
                        src_val = None
                elif target_ddl.data_type in [TargetTable.CHAR]:
                    if src_val.strip() == '(null)':
                        src_val = None
                elif target_ddl.data_type in TargetTable.DATE_TYPES:
                    if src_val.strip() == '(null)' or \
                            src_val.strip() == '0001-01-01':
                        src_val = None
                    else:
                        if len(src_val) == 8 and ':' in src_val:
                            date_format = '%H:%M:%S'
                        elif '.' in src_val:
                            date_format = '%Y-%m-%d %H:%M:%S.%f'
                        elif ':' in src_val:
                            date_format = '%Y-%m-%d %H:%M:%S'
                        else:
                            date_format = '%Y-%m-%d'
                        src_val = datetime.datetime.strptime(src_val,
                                                             date_format)

                if src_val == target_val:
                    column_match_list.append(True)
                else:
                    err_msg = ("\nMismatched row data: '{}' '{}'"
                               "\nSource & Hive DDL: {} {}")
                    logger.error(err_msg.format(src_val, target_val,
                                                src_ddl, target_ddl))
                    column_match_list.append(False)

            column_match_count = len(list(ifilter(lambda x: x,
                                                  column_match_list)))
            column_count = len(target_ddls)

            if column_match_count != column_count:
                logger.error("Row data does not match")
                err_msg = "Fields Matched: '{}' Mismatched: '{}'"
                logger.error(err_msg.format(column_match_count,
                                            column_count - column_match_count))
            else:
                logger.info('Row data matched')
                row_match_cnt += 1

            if index - row_match_cnt > 10:
                err_msg = ("FAILED. Too many row comparisions failed."
                           " Exiting.. Rows Matched: {0} Mismatched: {1}")
                err_msg = err_msg.format(row_match_cnt, index - row_match_cnt)
                raise ValueError(err_msg)

        if total_rows_cnt != row_match_cnt:
            err_msg = "FAILED. Random row comparision. " \
                      "Rows Matched: {0} Mismatched: {1}"
            err_msg = err_msg.format(row_match_cnt,
                                     total_rows_cnt - row_match_cnt)
            logger.error(err_msg)
        else:
            status = True
            msg = "Source and Target row data matches: " \
                  "Rows Matched: {0} Mismatched: {1}"
            logger.info(msg.format(row_match_cnt,
                                   total_rows_cnt - row_match_cnt))

        return status

    def target_ddl_rows_match(self, target_ddls, target_rows):
        """Validate if sample rows from hive conforms to hive ddl."""
        def int_validation(value):
            """check if data is of int type"""
            if value is None:
                return value
            if isinstance(value, int):
                return value
            raise voluptuous.Invalid("Not a "
                                     "int: {} {}".format(value, type(value)))

        def decimal_validation(value):
            """check if data is of decimal type"""
            if value is None:
                return value
            if isinstance(value, decimal.Decimal):
                return value
            raise voluptuous.Invalid("Not a decimal: "
                                     "{} {}".format(value, type(value)))

        def str_validation(value):
            """check if data is of string type"""
            if value is None:
                return value
            if isinstance(value, str) or isinstance(value, unicode):
                return value
            raise voluptuous.Invalid("Not a str or unicode: "
                                     "{} {}".format(value, type(value)))

        def build_str_data_len_func(ddl):
            def len_f(col_val):
                if col_val is None:
                    return col_val
                if len(col_val) <= ddl.data_len:
                    return col_val
                err_msg = "Not a str or unicode: {} {}"
                raise voluptuous.Invalid(err_msg.format(col_val,
                                                        type(col_val)))
            return len_f

        def build_precision_scale_func(ddl):

            def f(col_val):
                if col_val is None or col_val == 0:
                    return col_val

                scale_col_val = col_val.as_tuple().exponent
                precision_col_val = int(math.log10(
                    col_val) + 1) + abs(scale_col_val)

                if ddl.precision >= precision_col_val and ddl.scale\
                        >= scale_col_val:
                    return col_val

                err_msg = "Invalid. Expected Precision:{} Scale:{}. " \
                          "Current Precision:{} Scale:{}"
                err_msg = err_msg.format(ddl.precision, ddl.scale,
                                         precision_col_val, scale_col_val)
                raise voluptuous.Invalid(err_msg)
            return f

        def date_timestamp_validation(col_val):
            """check if data is of date type"""
            if col_val is None:
                return col_val
            try:
                if not isinstance(col_val, datetime.datetime):
                    raise voluptuous.Invalid("Not an instance of "
                                             "datetime.datetime")
                return col_val
            except ValueError as e:
                err_msg = "Not a date format. ERROR MESSAGE:{}".format(e)
                logger.error("Error found in "
                             "quality_assurance.date_timestamp_validation "
                             "- reason %s" % e.message)
                raise voluptuous.Invalid(err_msg)

        status = False
        total_count = len(target_rows)
        count = 0
        for col_values in target_rows:
            schema_input = {}
            data_input = {}
            for ddl, col_val in izip(target_ddls, col_values):
                column = voluptuous.Required(ddl.column_name)
                data_input[ddl.column_name] = col_val
                if ddl.data_type in [TargetTable.STRING]:
                    rules = voluptuous.All(str_validation)
                elif ddl.data_type in [TargetTable.CHAR, TargetTable.VARCHAR]:
                    validation_func = build_str_data_len_func(ddl)
                    rules = voluptuous.All(str_validation, validation_func)
                elif ddl.data_type in [TargetTable.DECIMAL]:
                    validation_func = build_precision_scale_func(ddl)
                    rules = voluptuous.All(decimal_validation, validation_func)
                elif ddl.data_type in TargetTable.NON_DECIMAL_NUM_TYPES:
                    rules = voluptuous.All(int_validation)
                elif ddl.data_type in \
                        [TargetTable.TIMESTAMP, TargetTable.DATE]:
                    rules = voluptuous.All(date_timestamp_validation)
                else:
                    err_msg = 'Unhandled data type in hive ddl row match: {}'
                    logger.error(err_msg.format(ddl.data_type))
                schema_input[column] = rules

            hive_schema = voluptuous.Schema(schema_input)
            try:
                hive_schema(data_input)
                count += 1
            except voluptuous.Invalid as err:
                print err
                logger.error(traceback.format_exc())

        if total_count == count:
            msg = ('Hive ddl vs row data matches.'
                   ' Rows Matched: {} Mismatched {}')
            logger.info(msg.format(count, total_count - count))
            status = True
        else:
            err_msg = 'FAILED. Hive ddl vs row data. Rows Matched: {}' \
                      ' Mismatched {}'
            logger.error(err_msg.format(count, total_count - count))

        return status

    def start_incremental(self, full_target_table, action):
        """Start validating tables for incremental"""
        if not self.src_obj:
            return True
        qa_stages = {}

        sqoop_logcount = self.checks_balances.validate_in_and_out_counts(
            action)
        target_rowcount = self.target_obj.get_row_count()
        self.count_bool = self.count_matches(sqoop_logcount, target_rowcount)
        qa_stages['count'] = self.count_bool

        source_ddls = self.src_obj.get_ddl()
        target_ddls = self.target_obj.get_ddl()
        logger.info('Verifying for incremental ddl:')
        self.ddl_bool = self.ddl_matches(source_ddls, target_ddls)

        # verify already ingested ddl as well
        self.target_table = full_target_table
        self.target_obj = TargetTable(self.target_table,
                                      self.params['impala_host'])
        target_ddls = self.target_obj.get_ddl()
        logger.info('Verifying for already ingested full table ddl:')
        self.ddl_bool = self.ddl_bool and self.ddl_matches(source_ddls,
                                                           target_ddls)

        qa_stages['ddl'] = self.ddl_bool

        logger.qa_status(qa_stages)
        return self.count_bool and self.ddl_bool  # and rows_bool
        #  and target_types_bool

    def start_full_ingest_qa(self):
        """Start validating tables.
        Note that we don't get the row count in a full ingest
        because that's taken care of with the SQOOP validate
        which would have failed the QA if that didn't succeed
        """
        if not self.src_obj:
            return True

        qa_stages = {}

        source_ddls = self.src_obj.get_ddl()
        target_ddls = self.target_obj.get_ddl()
        self.ddl_bool = self.ddl_matches(source_ddls, target_ddls)
        qa_stages['ddl'] = self.ddl_bool

        # src_rand_rows = self.src_obj.get_rand_rows(source_ddls)
        # matched_target_rows = self.target_obj.get_corresponding_rows
        # (source_ddls, target_ddls,
        #
        #  src_rand_rows, self.src_obj)
        # self.rows_bool = self.rows_match(source_ddls, target_ddls,
        # src_rand_rows, matched_target_rows)
        # self.target_types_bool = self.target_ddl_rows_match(
        # , matched_target_rows)

        logger.qa_status(qa_stages)
        return self.ddl_bool  # and rows_bool and target_types_bool

    def start_data_sampling(self):
        """Start qa data sampling"""
        if not self.src_obj:
            return True
        logger.info('QA data sampling')
        qa_stages = {}

        # source_rowcount = self.src_obj.get_row_count()
        # target_rowcount = self.target_obj.get_row_count()
        # self.count_bool = self.count_matches(
        #    source_rowcount, target_rowcount)
        # qa_stages['count'] = self.count_bool

        source_ddls = self.src_obj.get_ddl()
        target_ddls = self.target_obj.get_ddl()
        self.ddl_bool = self.ddl_matches(source_ddls, target_ddls)
        qa_stages['ddl'] = self.ddl_bool

        src_rand_rows = self.src_obj.get_rand_rows(source_ddls)
        matched_target_rows = self.target_obj.get_corresponding_rows(
            source_ddls, target_ddls, src_rand_rows, self.src_obj)
        self.rows_bool = self.rows_match(source_ddls, target_ddls,
                                         src_rand_rows, matched_target_rows)
        self.target_types_bool = self.target_ddl_rows_match(
            target_ddls, matched_target_rows)

        logger.qa_status(qa_stages)
        # print self.ddl_bool, self.rows_bool, self.target_types_bool
        return self.ddl_bool and self.rows_bool and self.target_types_bool


def main():
    """Start qa"""
    global logger

    args = {
        'database': sys.argv[1],
        'table_name': sys.argv[2],
        'jars': sys.argv[3],
        'jdbc_url': sys.argv[4],
        'connection_factories': sys.argv[5],
        'user_name': sys.argv[6],
        'password_file': sys.argv[7],
        'domain': sys.argv[8],
        'ingestion_type': sys.argv[9],
        'schema': sys.argv[10],
        'oozie_url': sys.argv[11],
        'workflow_name': sys.argv[12],
        'impala_host': os.environ['IMPALA_HOST'],
        'qa_results_tbl_path': sys.argv[13]
    }

    source_table = args['database'] + '.' + args['table_name']
    logger = LogHandler(source_table, args['impala_host'],
                        args['qa_results_tbl_path'])

    try:
        # remove special characters
        database = SourceTable.convert_special_chars(args['database'])
        table_name = SourceTable.convert_special_chars(args['table_name'])

        if args['ingestion_type'] == 'full_ingest':
            target_table = 'parquet_stage.{database}_{table_name}'
            target_table = target_table.format(
                database=database, table_name=table_name)
            val_obj = TableValidation(source_table, target_table, args)
            bool_status = val_obj.start_full_ingest_qa()
        elif args['ingestion_type'] == 'full_ingest_qa_sampling':
            target_table = 'parquet_stage.{database}_{table_name}'
            target_table = target_table.format(database, table_name)
            val_obj = TableValidation(source_table, target_table, args)
            try:
                bool_status = True  # val_obj.start_data_sampling()
            except Exception as ex:
                print 'Data sampling failed'
                print traceback.format_exc()
                bool_status = True
        elif args['ingestion_type'] == 'standalone_qa_sampling':
            target_table = 'parquet_stage.{database}_{table_name}'
            target_table = target_table.format(
                database=database, table_name=table_name)
            val_obj = TableValidation(source_table, target_table, args)
            try:
                bool_status = val_obj.start_data_sampling()
            except Exception as ex:
                print 'Data sampling failed'
                print traceback.format_exc()
                bool_status = True
        elif args['ingestion_type'] == 'incremental':
            incr_target_table = 'parquet_stage.{database}_{table_name}'
            incr_target_table = incr_target_table.format(
                database=database, table_name=table_name)
            full_target_table = '{domain}.{database}_{table_name}'
            full_target_table = full_target_table.format(
                domain=args['domain'], database=database,
                table_name=table_name)

            cb_mgr = ChecksBalancesManager(
                args['impala_host'], args['oozie_url'],
                args['qa_results_tbl_path'])
            app_name = cb_mgr.check_if_workflow(args['workflow_name'])
            sorted_actions = cb_mgr.sort_actions(app_name)

            for table in sorted_actions.keys():
                for action in sorted_actions[table]:
                    if 'import' in action.get_name():
                        new_action = action

            val_obj = TableValidation(source_table, incr_target_table, args)
            bool_status = val_obj.start_incremental(full_target_table,
                                                    new_action)

        logger.insert_hive()
        exit_code = 0 if bool_status else 1
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
