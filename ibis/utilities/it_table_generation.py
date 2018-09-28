"""IT table generation with auto load, mappers and split by calculation."""
import gc
from multiprocessing import Pool
import os
import re
import signal
import subprocess
import time
import traceback
import numpy

from ibis.custom_logging import get_logger
from ibis.inventory.it_inventory import ITInventory
from ibis.inventory.request_inventory import OPTIONAL_FIELDS
from ibis.model.table import ItTable
from ibis.utilities.file_parser import parse_file_by_sections
from ibis.utilities.sqoop_helper import ORACLE, DB2, TERADATA, SQLSERVER, \
    MYSQL, POSTGRESQL

REQ_KEYS = ['source_database_name', 'source_table_name', 'mappers', 'jdbcurl',
            'db_username', 'password_file']
MAPPERS_ROW_COUNT_THRESHOLD = 250000
# MAPPERS_ROW_COUNT_THRESHOLD = 250


def parallel_sqoop_output(info):
    """Fetch distinct column count
    For sake of multiprocessing.Pool, this needs to be a top level function
    """
    col_quality_list = []
    cfg_mgr, it_table, column_name, query = info[0], info[1], info[2], info[3]
    it_table_obj = ItTable(it_table, cfg_mgr)
    source_obj = SourceTable(cfg_mgr, it_table_obj)
    returncode, output, err = source_obj.eval(query)
    if returncode == 0:
        _, groupby_counts = source_obj.fetch_rows_sqoop(output)
    else:
        source_obj.logger.error(err)
        raise ValueError(err)
    # collect counts per bin, calculate relevant stats on them, and sort
    # to most preferred first
    bin_counts = [int(item) for sublist in groupby_counts for item in sublist]
    num_groups = len(bin_counts)
    bin_counts_arr = numpy.array(bin_counts)
    std_deviation = numpy.std(bin_counts_arr)
    # print column_name, os.getpid(), os.getppid()
    # memory_usage_ps()
    del groupby_counts
    del bin_counts
    del bin_counts_arr
    gc.collect()
    # memory_usage_ps()
    # print '--' * 50
    col_quality_list.append((column_name, std_deviation, num_groups))
    return col_quality_list


class RunningTooLongError(Exception):
    """If process runs for too long"""

    def __init___(self, dErrorArguments):
        """init"""
        Exception.__init__(self, "{0}".format(dErrorArguments))
        self.dErrorArguments = dErrorArguments


class SourceTable(object):
    """Common operations for sql tables."""

    def __init__(self, cfg_mgr, it_table):
        """Init.
        Args:
            it_table(ibis.model.table.ItTable): instance of ItTable
        """
        self.cfg_mgr = cfg_mgr
        self.it_table_obj = it_table
        self.db = it_table.database.upper()
        self.table = it_table.table_name.upper()
        self.domain = it_table.domain
        self.user_name = it_table.username
        self.password_file = it_table.password_file
        self.driver = it_table.connection_factories
        self.jdbc_url = it_table.jdbcurl
        self.mappers = it_table.mappers
        self.db_env = it_table.db_env
        self.logger = get_logger(self.cfg_mgr)

    def _close_logs(self):
        """destruct"""
        pass
        # handlers = self.logger.handlers[:]
        # for handler in handlers:
        #    handler.close()
        #    self.logger.removeHandler(handler)

    def eval(self, query):
        """Fetch data for the query.
        Args:
            query: sql statement to query against data source
        Returns:
            returncode: 0 is success and anything else is failure
            output: sqoop output as it is_primary_key
            err: sqoop error if any
        """
        if 'jceks' in self.password_file:
            jceks, password_alias = self.password_file.split('#')
            cmd_list = ['sqoop-eval',
                        '-D hadoop.security.credential.provider.path=' + jceks,
                        '--driver', self.driver, '--verbose', '--connect',
                        self.jdbc_url,
                        '--query', query, '--username', self.user_name,
                        '--password-alias', password_alias]
        else:
            cmd_list = ['sqoop-eval', '--driver', self.driver, '--verbose',
                        '--connect',
                        self.jdbc_url, '--query', query, '--username',
                        self.user_name,
                        '--password-file', self.password_file]
        proc = subprocess.Popen(cmd_list, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        output, err = proc.communicate()
        return proc.returncode, output, err

    def _clean_query_result(self, output):
        """Match characters between two | characters. Ex: | anything |.
        Args:
            output: sqoop output
        Returns:
            Matched string between |abc|
        """
        pattern = re.compile('\|.*\|')
        return pattern.findall(output)

    def fetch_rows_sqoop(self, output, strip_col_val=True,
                         filter_columns=None):
        """Clean up table output received through sqoop.
        Args:
            output(str): sqoop output string
            strip_col_val(bool): if column values have to be striped
            off white spaces
            filter_columns(list): selected list of zero indexed column
             numbers which will be
                         included in returned row data
        Returns:
            column labels and row data
        """
        # t1 = time.time()
        # print 1
        # humanize_bytes(sys.getsizeof(output))
        raw_data = self._clean_query_result(output)
        # memory_usage_ps()
        del output
        gc.collect()
        # memory_usage_ps()
        # t2 = time.time()
        # print 2
        # time_calc(t2, t1)
        row_data = []
        # memory_usage_ps()
        for line in raw_data:
            column_data = line.split('|')
            # ignore the first and last items due to split
            column_data = column_data[1:-1]
            row_data.append(column_data)

        # memory_usage_ps()
        # t3 = time.time()
        # print 3
        # time_calc(t3, t2)
        # first item is labels in table output
        column_labels = row_data[0]
        column_labels = [label.strip() for label in column_labels]
        num_column_labels = len(column_labels)
        # row data starts from second item in table output
        row_values = row_data[1:]
        # remove the extra space in the start and end of string
        # which is inserted by sqoop output table
        row_values = [[col_val[1:-1] for col_val in row] for row in row_values]
        # t4 = time.time()
        # print 4
        # time_calc(t4, t3)
        if filter_columns:
            pop_columns = []
            for i in range(num_column_labels):
                if i not in filter_columns:
                    pop_columns.append(i)
            pop_columns.sort(reverse=True)
            for index in pop_columns:
                column_labels.pop(index)
                temp_row_values = []
                for row in row_values:
                    row.pop(index)
                    temp_row_values.append(row)
                row_values = temp_row_values
        if strip_col_val:
            row_values = [[val.strip() for val in row] for row in row_values]
        return column_labels, row_values

    def build_row_count_query(self):
        """Return row count query."""
        return "SELECT COUNT(*) FROM {db}.{table}".format(db=self.db,
                                                          table=self.table)

    def get_table_count(self):
        """Fetch row count of source table.
        Returns: row count of table
        """
        row_count = -1
        query = self.build_row_count_query()
        returncode, output, err = self.eval(query)
        if returncode == 0:
            _, row_data = self.fetch_rows_sqoop(output)
            row_count = int(row_data[0][0])
        else:
            self.logger.error(err)
            raise ValueError(err)

        return row_count

    def find_load(self, row_count):
        """Generate load string based on row count."""
        load = ''
        if row_count >= 0 and row_count < 2000000:
            load = '100'
        elif row_count >= 2000000 and row_count <= 100000000:
            load = '010'
        elif row_count > 100000000:
            load = '001'
        return load

    def find_mappers(self, row_count, load=None):
        """Find mappers based on row count."""
        if row_count > MAPPERS_ROW_COUNT_THRESHOLD:
            if self.mappers > 1:  # Default mappers: 1
                return str(self.mappers)
            elif not load:
                load = self.find_load(row_count)
            if 'oracle' in self.jdbc_url.lower():
                return str(self.cfg_mgr.oracle_mappers[load])
            if 'teradata' in self.jdbc_url.lower():
                return str(self.cfg_mgr.teradata_mappers[load])
            if 'db2' in self.jdbc_url.lower():
                return str(self.cfg_mgr.db2_mappers[load])
            if 'sqlserver' in self.jdbc_url.lower():
                return str(self.cfg_mgr.sqlserver_mappers[load])
            if 'postgresql' in self.jdbc_url.lower():
                return str(self.cfg_mgr.postgresql_mappers[load])
            if 'mysql' in self.jdbc_url.lower():
                return str(self.cfg_mgr.mysql_mappers[load])
        return '1'

    def get_auto_values(self, timeout=45):
        """Determine auto it table"""
        time_start = time.time()
        self.logger.info("Timeout set to: {0}".format(timeout))
        # Set the signal handler for alarm
        signal.signal(signal.SIGALRM, self.timeout_handler)
        signal.alarm(timeout)

        try:
            row_count = self.get_table_count()
            load = self.find_load(row_count)
            mappers = self.find_mappers(row_count, load)
            split_by_column = self.find_split_by_column(mappers)
            # if done within time, switch off timer
            signal.alarm(0)
        except RunningTooLongError as err:
            err_msg = 'Auto values taking too long, skipping calculation' \
                      ' - error reason  {0}'
            err_msg = err_msg.format(err.message)
            self.logger.error(err_msg)
            load = self.it_table_obj.load
            mappers = self.it_table_obj.mappers
            split_by_column = self.it_table_obj.split_by
        time_end = time.time()
        self.logger.info("Time taken for split_by: {0} seconds".format(
            time_end - time_start))
        return load, mappers, split_by_column

    def generate_it_table(self, timeout):
        """Generate it table template output.
        Returns: it-table-template generated, with appropriate split-by value
        """
        it_table_template = (
            "[Request]\njdbcurl:{jdbc_url}\ndb_username:{user_name}\n"
            "password_file:{password_file}\ndomain:{domain}\n"
            "source_database_name:{db}\nsource_table_name:{table}"
            "load:{frequency}{load}\nmappers:{mappers}\nsplit_by:{split_by}\n"
            "\ndb_env:{db_env}\n")
        load, mappers, split_by_column = self.get_auto_values(timeout)

        if split_by_column:
            status = 'auto'
        else:
            status = 'manual'

        it_table = it_table_template.format(
            frequency=self.it_table_obj.frequency, load=load, mappers=mappers,
            split_by=split_by_column, db=self.db, table=self.table,
            domain=self.domain,
            jdbc_url=self.jdbc_url, user_name=self.user_name,
            password_file=self.password_file, db_env=self.db_env)
        return status, it_table

    def build_group_by_count_query(self, column_name):
        """Build group by column count query."""
        query = "SELECT COUNT({column_name}) FROM {db}.{table} GROUP" \
                " BY {column_name}"
        query = query.format(db=self.db, table=self.table,
                             column_name=column_name)
        return query

    def timeout_handler(self, signum, frame):
        """called when it takes too long to run based on timeout"""
        self.logger.info('Signal handler called with signal %s' % signum)
        raise RunningTooLongError("Time out! Running for too long")

    def find_split_by_column(self, num_mappers):
        """Find out split by.
        :param num_mappers(str): number of mappers
        :return: a split by column
        """
        split_by_column = ''
        try:
            # if int(num_mappers) <= 1:
            #    msg = 'Table: {0} - No need of split by as table has
            #  less rows'.format(self.table)
            #    self.logger.warning(msg)
            #    return split_by_column

            primary_key = self.find_primary_key()

            if primary_key:
                msg = 'Table: {0} - Primary key is the split by column'.format(
                    self.table)
                self.logger.info(msg)
                split_by_column = primary_key
                return split_by_column
            else:
                self.logger.warning('Could not find primary key!')

            query = self.build_column_type_query()
            returncode, output, err = self.eval(query)
            if returncode == 0:
                _, column_types = self.fetch_rows_sqoop(output)
            else:
                self.logger.error(err)
                raise ValueError(err)

            col_quality_list = self.get_groupby_counts(column_types)
            if col_quality_list:
                col_quality_list.sort(key=lambda item: item[1])
                # make the distributions larger than the number of mappers
                col_quality_list = [x for x in col_quality_list if
                                    x[2] > int(num_mappers)]
                # get the first column name from the sorted list
                if len(col_quality_list) > 0:
                    split_by_column = col_quality_list[0][0]
                    msg = "Found split by using groupby: '{0}', old: '{1}'"
                    msg = msg.format(split_by_column,
                                     self.it_table_obj.split_by)
                    self.logger.info(msg)
        except Exception, msg:
            self.logger.error(msg)
            self.logger.error(traceback.format_exc())
        return split_by_column

    def get_groupby_counts(self, column_types):
        """Fetch group by column count"""
        pool_info = []
        col_quality_list = []
        for row in column_types:
            column_name = row[0]
            query = self.build_group_by_count_query(column_name)
            pool_info.append((self.cfg_mgr, self.it_table_obj.get_meta_dict(),
                              column_name, query))

        pool_obj = Pool(processes=1)
        counts_list = pool_obj.map(parallel_sqoop_output, pool_info)
        for each in counts_list:
            col_quality_list = col_quality_list + each
        # self.logger.info(len(counts_list))
        pool_obj.terminate()
        return col_quality_list

    def _find_primary_key(self):
        """Find primary key column in the table.
        Returns: returns row data. Db specific methods
        """
        row_data = None
        query = self.build_primary_key_query()
        returncode, output, err = self.eval(query)
        if returncode == 0:
            _, row_data = self.fetch_rows_sqoop(output)
        else:
            self.logger.error(err)
        return row_data

    def write_it_table_files(self, write_path, it_table_string):
        """Write it table to file.
        :param filename: the name of the file to create
        :param it_table_string: the contents of the file to be written
        :return: writes out a file
        """
        if it_table_string:
            file_permission = 0774
            with open(write_path, 'w') as file_obj:
                file_obj.write(it_table_string)
                self.logger.info(
                    "Generated IT table file {0}".format(write_path))
            os.chmod(write_path, file_permission)


class OracleTable(SourceTable):
    """Oracle specific methods."""

    split_by_types = []

    def __init__(self, cfg_mgr, it_table):
        """Init."""
        super(self.__class__, self).__init__(cfg_mgr, it_table)

    def find_primary_key(self):
        """Find primary key.
        Returns: primary key, if there is one
        """
        return None

    def build_column_type_query(self):
        """Build column type query."""
        query = ("SELECT COLUMN_NAME, DATA_TYPE FROM all_tab_columns"
                 " WHERE OWNER='{db}' AND TABLE_NAME='{table}'")
        query = query.format(db=self.db, table=self.table)
        return query

    def find_mappers(self, row_count, load=None):
        """Find mappers based on row count."""
        # OraOop connector requires at least two mappers.
        if row_count > MAPPERS_ROW_COUNT_THRESHOLD:
            if self.mappers > 1:
                return str(self.mappers)
            elif not load:
                load = self.find_load(row_count)
                return str(self.cfg_mgr.oracle_mappers[load])
        return '2'

    def find_split_by_column(self, num_mappers):
        """Find split by column, oracle has none."""
        self.logger.info('Oracle does not need split by')
        return ""


class DB2Table(SourceTable):
    """DB2 specific methods."""

    split_by_types = ['INTEGER']

    def __init__(self, cfg_mgr, it_table):
        """Init."""
        super(self.__class__, self).__init__(cfg_mgr, it_table)

    def build_is_table_udb2(self):
        """Query to validate DB table object in uDB2 """
        query = ("SELECT TABNAME FROM SYSCAT.TABLES "
                 "WHERE TABSCHEMA = '{db}' "
                 "AND TABNAME  = '{table}' AND TYPE = 'T'")
        query = query.format(db=self.db,
                             table=self.table)
        return query

    def build_is_table_db2(self):
        """Query to validate DB table object in DB2 """
        query = ("SELECT NAME FROM SYSIBM.SYSTABLES "
                 "WHERE CREATOR = '{db}' AND "
                 "NAME = '{table}' AND TYPE = 'T'")
        query = query.format(db=self.db,
                             table=self.table)
        return query

    def is_db2_table(self):
        """Validate is DB object is a table
        in order to find a primary or unique key """
        is_valid = False
        query = self.build_is_table_db2()
        DB2_table = self.process_split_rule(query,
                                            "DB object is not a DB2 table")
        if not DB2_table or not any(DB2_table):
            query = self.build_is_table_udb2()
            uDB2_table = self.process_split_rule(query,
                                                 "DB object is not a "
                                                 "uDB2 table")
            if uDB2_table and any(uDB2_table):
                is_valid = True
        else:
            is_valid = True
        return is_valid

    def build_unique_key_db2_query(self):
        """Query to validate DB table object in DB2 """
        query = ("SELECT COLNAME FROM SYSIBM.SYSKEYS K, "
                 "SYSIBM.SYSINDEXES I WHERE I.NAME = K.IXNAME "
                 "AND I.CREATOR = K.IXCREATOR AND "
                 "I.TBCREATOR = '{db}' AND "
                 "I.TBNAME = '{table}' AND "
                 "I.UNIQUERULE IN ('U','P')")
        query = query.format(db=self.db,
                             table=self.table)
        return query

    def build_unique_key_udb2_query(self):
        """Query to validate DB table object in DB2 """
        query = ("SELECT COLNAMES FROM SYSCAT.INDEXES "
                 "WHERE TABSCHEMA) = '{db}' AND "
                 "TABNAME = '{table}' "
                 "AND UNIQUERULE IN ('U','P')")
        query = query.format(db=self.db,
                             table=self.table)
        return query

    def build_primary_key_query(self):
        """Build primary key query."""
        query = ("SELECT NAME  FROM SYSIBM.SYSCOLUMNS "
                 "WHERE TBCREATOR = '{db}' AND (TBNAME) = '{table}' "
                 "and NULLS='N' AND "
                 "COLTYPE IN ('CHAR','INTEGER','BIGINT','LONGVAR', "
                 "'SMALLINT','ROWID','VARCHAR') "
                 "AND COLCARD = (SELECT MAX(COLCARD) "
                 "FROM SYSIBM.SYSCOLUMNS WHERE TBCREATOR = '{db}' "
                 "AND (TBNAME) = '{table}' "
                 "AND NULLS='N' AND COLTYPE IN ('CHAR','INTEGER', "
                 "'BIGINT','LONGVAR','SMALLINT','ROWID','VARCHAR')) "
                 "AND COLNO =  (SELECT MIN(COLNO) FROM SYSIBM.SYSCOLUMNS "
                 "WHERE TBCREATOR = '{db}' AND (TBNAME) = '{table}' "
                 "AND NULLS='N' AND COLTYPE in ('CHAR','INTEGER','BIGINT', "
                 "'LONGVAR','SMALLINT','ROWID','VARCHAR') "
                 "AND COLCARD = (SELECT MAX(COLCARD) "
                 "FROM SYSIBM.SYSCOLUMNS WHERE TBCREATOR = '{db}' "
                 "AND (TBNAME) = '{table}' AND NULLS='N' AND COLTYPE IN "
                 "('CHAR','INTEGER','BIGINT','LONGVAR','SMALLINT', "
                 "'ROWID','VARCHAR')))")
        query = query.format(db=self.db,
                             table=self.table)
        return query

    def find_primary_key(self):
        """Find primary key.
        return: primary key, if there is one
        """
        primary_key = None
        row_data = super(self.__class__, self)._find_primary_key()
        if row_data and any(row_data):
            primary_key = row_data[0][0]
        return primary_key

    def find_unique_key(self):
        """Find unique key from int columns in indexes
        return: unique key, if there is one
        """
        unique_key = "no-split"
        int_columns = self.find_int_columns()
        if not int_columns or not any(int_columns):
            self.logger.info("Split by cannot be assigned, not able to find "
                             "integer columns in {db}.{table}"
                             .format(db=self.db,
                                     table=self.table))
        else:
            query = self.build_unique_key_db2_query()
            colnames = self.process_split_rule(query,
                                               "No unique columns "
                                               "found in db2 indexes")
            if not colnames or not any(colnames):
                query = self.build_unique_key_udb2_query()
                colnames = self.process_split_rule(query,
                                                   "No unique columns "
                                                   "found in udb2indexes")
            if colnames and any(colnames):
                for name in int_columns[0]:
                    for item in colnames:
                        print "name ", name
                        if name in item:
                            unique_key = name
                            break
            else:
                unique_key = int_columns[0][0]
                self.logger.error("No unique key found,  "
                                  "Split by suggested : {sp}"
                                  .format(sp=unique_key))
        return unique_key

    def find_int_columns(self):
        query = self.build_column_int_query()
        int_columns = self.process_split_rule(query,
                                              "No integer columns found")
        return int_columns

    def build_column_type_query(self):
        """Build column type query."""
        query = (
            "SELECT NAME, COLTYPE FROM SYSIBM.SYSCOLUMNS "
            "WHERE TBCREATOR = '{db}' "
            "AND TBNAME ='{table}'")
        query = query.format(db=self.db,
                             table=self.table)
        return query

    def build_column_int_query(self):
        """Build query to get int or bigint columns."""
        query = ("SELECT NAME FROM SYSIBM.SYSCOLUMNS "
                 "WHERE TBCREATOR = '{db}' AND "
                 "TBNAME = '{table}' and COLTYPE like '%INT%'")
        query = query.format(db=self.db,
                             table=self.table)
        return query

    def get_split(self):
        """  Return split by value for the db.table
        """
        unique_key = "no-split"

        # Rule 1 - Validate if DB Object is a table to reach split by
        if self.is_db2_table():
            # Rule 2 - find integer primary key
            unique_key = self.find_primary_key()
            if unique_key:
                self.logger.info("Split by column found in DB2 "
                                 "table primary key {0}"
                                 .format(unique_key))
            else:
                # Rule 3 - find unique key from integers
                unique_key = self.find_unique_key()
                if unique_key:
                    self.logger.info("Split by column found in DB2 "
                                     "table unique key index {0}"
                                     .format(unique_key))
                else:
                    unique_key = "no-split"
        else:
            int_columns = self.find_int_columns()
            if not int_columns or not any(int_columns):
                self.logger.info("Split by cannot be assigned, not able "
                                 "to find integer columns "
                                 "in {db}.{table}"
                                 .format(db=self.db,
                                         table=self.table))
            else:
                # DB2 views cannot have indexes or PKs,
                # should be a DBA decision
                unique_key = int_columns[0][0]
                self.logger.info("DB object is a view, DB2 cannot create "
                                 "an index for a view. split_by "
                                 "property should be defined in "
                                 "request file. Split by suggested : "
                                 "{sp}"
                                 .format(sp=unique_key))

        return unique_key

    def process_split_rule(self, query, step_err_message):
        """  Process split by rule
        """
        process_result = None
        try:
            returncode, output, err = self.eval(query)
            if returncode == 0:
                _, result = self.fetch_rows_sqoop(output)
                if any(result):
                    process_result = result
                else:
                    self.logger.error(step_err_message)
            else:
                self.logger.error(err)
        except IndexError as err:
            self.logger.error("Sqoop error - no access to DB2 Sys views "
                              "(Index error)")
        return process_result


class TeradataTable(SourceTable):
    """Teradata specific methods."""

    split_by_types = ['I', 'D']

    def __init__(self, cfg_mgr, it_table):
        """Init."""
        super(self.__class__, self).__init__(cfg_mgr, it_table)

    def build_primary_key_query(self):
        """Build primary key query."""
        query = 'HELP COLUMN {db}.{table}.*;'.format(db=self.db,
                                                     table=self.table)
        return query

    def find_primary_key(self):
        """Find primary key.

        Returns: primary key, if there is one
        """
        row_data = super(self.__class__, self)._find_primary_key()
        # col 13 is 'Primary?' which signifies a primary key
        primary_key = row_data[0][0] if (row_data[0][13] == 'P') else None
        return primary_key

    def build_column_type_query(self):
        """Build column type query."""
        query = "HELP COLUMN {db}.{table}.*;".format(db=self.db,
                                                     table=self.table)
        return query

    def get_split(self):

        query = """select  databasename source_database_name,
        (tablename) source_table_name,
        columnname,
        case
         when indextype='P' and \
         indexnumber=1 then 2
         when indextype='P' and \
         indexnumber=2 then 4
         when indextype='Q' and \
         indexnumber=1 then 1
         when indextype='Q' and \
         indexnumber=2 then 3
         when indextype='S'  then 6
         when indextype='K' then 5
        else 7
        end as Split_By_Find
        from dbc.indices
        where
        databasename = '{db_name}'
        and tablename = '{table_name}'
        order by Split_By_Find"""

        db = self.db
        table = self.table

        server_name = filter(lambda server: server.lower() in
                             self.jdbc_url.lower(),
                             self.cfg_mgr.teradata_server)
        if server_name:
            server_name = self.cfg_mgr.teradata_server[server_name[0]]
            hdfs_query = """ select columnname from ibis.teradata_split_{0} \
            where lower(databasename) = '{1}' and lower(tablename) = '{2}'"""
            hdfs_query = hdfs_query.format(server_name, db.lower(),
                                           table.lower())
            run_query = ITInventory(self.cfg_mgr)
            results = run_query.get_rows(hdfs_query)
            if results:
                return results[0][0]
            else:
                self.logger.warning('No split_by found:\
                 {0}'.format(hdfs_query))
                return "no-split"
        else:
            query = query.format(table_name=table, db_name=db)
            returncode, output, err = self.eval(query)
            if returncode == 0:
                _, result = self.fetch_rows_sqoop(output)
                if result is None or len(result) == 0:
                    self.logger.warning('No split_by found: {0}'.format(query))
                    return "no-split"
                else:
                    if result[0][3] == '7':
                        int_bigint_query = """SELECT columnname
                        FROM
                          (SELECT columnname,
                                  rank() over (partition BY columntype
                                               ORDER BY columnid) rnk
                           FROM dbc.columns
                           WHERE tablename = '{table_name}'
                             AND databasename = '{db_name}'
                             AND columntype IN ('I',
                                                'I8') )a
                        WHERE rnk=1""".format(table_name=table, db_name=db)
                        returncode, output, err = self.eval(int_bigint_query)
                        if returncode == 0:
                            _, result = self.fetch_rows_sqoop(output)
                            if result is None:
                                self.logger.error("No split by assigned \
                                and one could not be found")
                                raise ValueError("No split by assigned \
                                and one could not be found")
                            if result:
                                result = result[0][0]
                                return result
                            else:
                                _msg = 'No split_by found: {0}'.format(query)
                                self.logger.warning(_msg)
                                return "no-split"
                        else:
                            self.logger.error(output)
                            self.logger.error(err)
                    else:
                        return result[0][2]
            else:
                self.logger.error(output)
                self.logger.error(err)

        raise ValueError("No split by assigned and one could not be found")


class SqlServerTable(SourceTable):
    """MS SQL specific methods."""

    split_by_types = []

    def __init__(self, cfg_mgr, it_table):
        """Init."""
        super(self.__class__, self).__init__(cfg_mgr, it_table)

    def build_primary_key_query(self):
        """Build primary key query."""
        # note that SqlServer does not have a way of specifying database
        # for this method of finding primary key
        query = ("SELECT COL_NAME(ic.OBJECT_ID,ic.column_id) AS ColumnName"
                 " FROM sys.indexes AS i"
                 " INNER JOIN sys.index_columns AS ic"
                 " ON i.OBJECT_ID = ic.OBJECT_ID"
                 " AND i.index_id = ic.index_id"
                 " WHERE i.is_primary_key = 1"
                 " AND OBJECT_NAME(ic.OBJECT_ID) = '{table}'")
        query = query.format(table=self.table)
        return query

    def find_primary_key(self):
        """Find primary key.

        Returns: primary key, if there is one
        """
        row_data = super(self.__class__, self)._find_primary_key()
        primary_key = row_data[0][0] if len(row_data) > 0 else None
        return primary_key

    def build_column_type_query(self):
        """Build column type query."""
        query = (
            "SELECT COLUMN_NAME, DATA_TYPE "
            "FROM {database}.INFORMATION_SCHEMA.COLUMNS"
            " WHERE TABLE_CATALOG='{database}' and TABLE_NAME='{table}'")
        query = query.format(database=self.db, table=self.table)
        # print query
        # print os.getpid()
        return query

    def get_split(self):

        db = self.db
        table = self.table
        query = """SELECT column_name as PRIMARYKEYCOLUMN
        FROM {database}.INFORMATION_SCHEMA.table_constraints AS TC
        INNER JOIN
            {database}.INFORMATION_SCHEMA.key_column_usage AS KU
                  ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND
                     TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND
                     KU.table_name='{table_name}'
        ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION;"""

        query = query.format(table_name=table,
                             database=db)
        returncode, output, err = self.eval(query)
        if returncode == 0:
            _, result = self.fetch_rows_sqoop(output)
            if not result:
                int_or_bigint_query = """SELECT DISTINCT TOP (1)
                sys.tables.object_id AS TableId,
                sys.columns.column_id AS ColumnId,
                sys.columns.name AS ColumnName,
                sys.types.name AS TypeName,
                sys.columns.precision AS NumericPrecision,
                sys.columns.scale AS NumericScale,
                sys.columns.is_nullable AS IsNullable
                FROM
                sys.columns, sys.types, sys.tables
                WHERE
                sys.tables.object_id = sys.columns.object_id AND
                sys.types.system_type_id = sys.columns.system_type_id AND
                sys.types.user_type_id = sys.columns.user_type_id AND
                sys.types.name like '%int%' AND
                sys.tables.name = '{table_name}'
                ORDER BY
                ColumnId asc""".format(table_name=table)
                returncode, output, err = self.eval(int_or_bigint_query)
                if returncode == 0:
                    _, result = self.fetch_rows_sqoop(output)
                    if result is None:
                        self.logger.error("No split by assigned \
                        and one could not be found")
                        raise ValueError("No split by assigned \
                        and one could not be found")
                    if result:
                        result = result[0][2]
                    else:
                        result = "no-split"
                    return result
                else:
                    self.logger.error(err)
            else:
                return result[0][0]
        else:
            self.logger.error(err)
        raise ValueError("No split by assigned and one could not be found")


class PostgreSqlTable(SourceTable):
    """MS SQL specific methods."""

    split_by_types = []

    def __init__(self, cfg_mgr, it_table):
        """Init."""
        super(self.__class__, self).__init__(cfg_mgr, it_table)

    def build_primary_key_query(self):
        """Build primary key query."""
        # note that SqlServer does not have a way of specifying database
        # for this method of finding primary key
        query = ("SELECT COL_NAME(ic.OBJECT_ID,ic.column_id) AS ColumnName"
                 " FROM sys.indexes AS i"
                 " INNER JOIN sys.index_columns AS ic"
                 " ON i.OBJECT_ID = ic.OBJECT_ID"
                 " AND i.index_id = ic.index_id"
                 " WHERE i.is_primary_key = 1"
                 " AND OBJECT_NAME(ic.OBJECT_ID) = '{table}'")
        query = query.format(table=self.table)
        return query

    def find_primary_key(self):
        """Find primary key.

        Returns: primary key, if there is one
        """
        row_data = super(self.__class__, self)._find_primary_key()
        primary_key = row_data[0][0] if len(row_data) > 0 else None
        return primary_key

    def build_column_type_query(self):
        """Build column type query."""
        query = (
            "SELECT COLUMN_NAME, DATA_TYPE "
            "FROM {database}.INFORMATION_SCHEMA.COLUMNS"
            " WHERE TABLE_CATALOG='{database}' and TABLE_NAME='{table}'")
        query = query.format(database=self.db, table=self.table)
        # print query
        # print os.getpid()
        return query

    def get_split(self):

        db = self.db
        table = self.table
        query = """SELECT column_name as PRIMARYKEYCOLUMN
        FROM {database}.INFORMATION_SCHEMA.table_constraints AS TC
        INNER JOIN
            {database}.INFORMATION_SCHEMA.key_column_usage AS KU
                  ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND
                     TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND
                     KU.table_name='{table_name}'
        ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION;"""

        query = query.format(table_name=table,
                             database=db)
        returncode, output, err = self.eval(query)
        if returncode == 0:
            _, result = self.fetch_rows_sqoop(output)
            if not result:
                int_or_bigint_query = """SELECT DISTINCT TOP (1)
                sys.tables.object_id AS TableId,
                sys.columns.column_id AS ColumnId,
                sys.columns.name AS ColumnName,
                sys.types.name AS TypeName,
                sys.columns.precision AS NumericPrecision,
                sys.columns.scale AS NumericScale,
                sys.columns.is_nullable AS IsNullable
                FROM
                sys.columns, sys.types, sys.tables
                WHERE
                sys.tables.object_id = sys.columns.object_id AND
                sys.types.system_type_id = sys.columns.system_type_id AND
                sys.types.user_type_id = sys.columns.user_type_id AND
                sys.types.name like '%int%' AND
                sys.tables.name = '{table_name}'
                ORDER BY
                ColumnId asc""".format(table_name=table)
                returncode, output, err = self.eval(int_or_bigint_query)
                if returncode == 0:
                    _, result = self.fetch_rows_sqoop(output)
                    if result is None:
                        self.logger.error("No split by assigned \
                        and one could not be found")
                        raise ValueError("No split by assigned \
                        and one could not be found")
                    if result:
                        result = result[0][2]
                    else:
                        result = "no-split"
                    return result
                else:
                    self.logger.error(err)
            else:
                return result[0][0]
        else:
            self.logger.error(err)
        raise ValueError("No split by assigned and one could not be found")


class MySQLTable(SourceTable):
    """MySQL specific methods."""

    split_by_types = []

    def __init__(self, cfg_mgr, it_table):
        """Init."""
        super(self.__class__, self).__init__(cfg_mgr, it_table)

    def build_primary_key_query(self):
        """Build primary key query."""
        # note that MySQL does not have a way of specifying database
        # for this method of finding primary key
        query = ("SELECT kcu.column_name"
                 " FROM information_schema.key_column_usage kcu"
                 " WHERE table_schema = '{database}'"
                 " AND constraint_name = 'PRIMARY'"
                 " AND table_name = '{table}'")
        query = query.format(database=self.db, table=self.table)
        return query

    def find_primary_key(self):
        """Find primary key.

        Returns: primary key, if there is one
        """
        row_data = super(self.__class__, self)._find_primary_key()
        primary_key = row_data[0][0] if len(row_data) > 0 else None
        return primary_key

    def build_column_type_query(self):
        """Build column type query."""
        query = ("SELECT COLUMN_NAME, DATA_TYPE FROM "
                 "INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG='{database}'"
                 " and TABLE_NAME='{table}'")
        query = query.format(database=self.db, table=self.table)
        return query

    def get_split(self):
        db = self.db
        table = self.table
        query = """SELECT COLUMN_NAME, TABLE_NAME
        FROM information_schema.key_column_usage
        WHERE
          TABLE_SCHEMA = '{db}'
          AND CONSTRAINT_NAME='PRIMARY'
        and table_name = '{table_name}'"""
        query = query.format(table_name=table,
                             db=db)
        returncode, output, err = self.eval(query)
        if returncode == 0:
            _, result = self.fetch_rows_sqoop(output)
            if not result:
                int_or_bigint_query = """SELECT COLUMN_NAME, COLUMN_TYPE \
                FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = \
                '{table_name}' AND TABLE_SCHEMA = '{db}' \
                and (DATA_TYPE = 'INT' or \
                DATA_TYPE = 'BIGINT');""".format(table_name=self.table, db=db)
                returncode, output, err = self.eval(int_or_bigint_query)
                if returncode == 0:
                    _, result = self.fetch_rows_sqoop(output)
                    if result is None:
                        self.logger.error("No split by assigned \
                        and one could not be found")
                        raise ValueError("No split by assigned \
                        and one could not be found")
                    if result:
                        result = result[0][0]
                    else:
                        result = "no-split"
                    return result
                else:
                    self.logger.error(err)
            else:
                return result[0][0]
        else:
            self.logger.error(err)
        raise ValueError("No split by assigned and one could not be found")


def get_src_obj(cfg_mgr, it_table):
    """Return instance specific to db
    Args:
        it_table: instance of ibis.model.table.ItTable
    """
    src_obj = None
    jdbc_url = it_table.jdbcurl
    if ORACLE in jdbc_url:
        src_obj = OracleTable(cfg_mgr, it_table)
    elif DB2 in jdbc_url:
        src_obj = DB2Table(cfg_mgr, it_table)
    elif TERADATA in jdbc_url:
        src_obj = TeradataTable(cfg_mgr, it_table)
    elif SQLSERVER in jdbc_url:
        src_obj = SqlServerTable(cfg_mgr, it_table)
    elif POSTGRESQL in jdbc_url:
        src_obj = PostgreSqlTable(cfg_mgr, it_table)
    elif MYSQL in jdbc_url:
        src_obj = MySQLTable(cfg_mgr, it_table)
    return src_obj


def get_auto_values(it_table, cfg_mgr):
    """Get auto values."""
    src_obj = get_src_obj(cfg_mgr, it_table)
    return src_obj.get_auto_values()


def create(tables_fh, cfg_mgr, timeout):
    """Start it table generation."""
    auto = ''
    manual = ''
    auto_fh = None

    if not timeout:
        timeout = 1800
    requests, msg, _ = parse_file_by_sections(tables_fh, '[Request]', REQ_KEYS,
                                              OPTIONAL_FIELDS)

    if msg:
        return auto_fh

    for request in requests:
        inv = ITInventory(cfg_mgr)
        db_table_dict = inv.get_table_mapping(
            request['source_database_name'].lower(),
            request['source_table_name'].lower(),
            request['db_env'].lower())
        if db_table_dict:
            it_table_obj = ItTable(db_table_dict, cfg_mgr)
        else:
            it_table_obj = ItTable(request, cfg_mgr)

        src_obj = get_src_obj(cfg_mgr, it_table_obj)
        status, it_table_str = src_obj.generate_it_table(timeout)

        if status == 'auto':
            auto += it_table_str + '\n'
        elif status == 'manual':
            manual += it_table_str + '\n'

    parent_dir = os.path.dirname(os.path.abspath(tables_fh.name))
    auto_file_path = parent_dir + '/AUTO_it-tables.txt'
    manual_file_path = parent_dir + '/MANUAL_it-tables.txt'
    src_obj.write_it_table_files(auto_file_path, auto)
    src_obj.write_it_table_files(manual_file_path, manual)

    if auto:
        auto_fh = open(auto_file_path, 'r')
    return auto_fh


class Get_Auto_Split(object):
    """
    Get Auto split by for a table
    """
    def __init__(self, cfg_mgr):
        self.cfg_mgr = cfg_mgr

    def get_split_by_column(self, it_table_obj):
        """
        get split by for it_table_obj
        """
        src_obj = get_src_obj(self.cfg_mgr, it_table_obj)
        return src_obj.get_split()


if __name__ == '__main__':
    tables_fh = open('tables.txt', 'r')
    create(tables_fh, "", 300)
