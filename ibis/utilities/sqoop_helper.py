"""Sqoop helper methods."""
import subprocess
import re
import sys
from ibis.custom_logging import get_logger

ORACLE = 'oracle'
DB2 = 'db2'
TERADATA = 'teradata'
SQLSERVER = 'sqlserver'
MYSQL = 'mysql'
POSTGRESQL = 'postgresql'
VALID_SOURCES = [ORACLE, DB2, SQLSERVER, TERADATA, MYSQL, POSTGRESQL]
DRIVERS = {
    "teradata": "com.teradata.jdbc.TeraDriver",
    "oracle": "com.quest.oraoop.OraOopManagerFactory",
    "db2": "com.cloudera.sqoop.manager.DefaultManagerFactory",
    "sqlserver": ['com.cloudera.sqoop.manager.DefaultManagerFactory',
                  'net.sourceforge.jtds.jdbc.Driver'],
    "mysql": "com.cloudera.sqoop.manager.DefaultManagerFactory",
    "postgresql": "com.cloudera.sqoop.manager.DefaultManagerFactory"}

# sql_query: sqoop_result
SQOOP_CACHE = {}
SQOOP_CACHE_VIEW = {}


class SqoopHelper(object):
    """Sqoop eval runner."""

    def __init__(self, cfg_mgr):
        self.cfg_mgr = cfg_mgr
        self.pattern_non_alphanumeric = re.compile(r'[^A-Za-z0-9_]')
        self.pattern_numeric_at_start = r'^\d'
        self.pattern_underscore_at_start = r'^_'
        self.logger = get_logger(self.cfg_mgr)

    def get_sqoop_eval(self):
        """Checks if sqoop-eval command exists
        :return sqoop_eval: Boolean
        """
        sqoop_eval = True
        try:
            subprocess.Popen('sqoop-eval', stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        except OSError as oe:
            sqoop_eval = False
            msg = "sqoop-eval command not available"
            self.logger.warning(msg)
            err_msg = "Error: sqoop-eval command not available - reason %s"
            err_msg = err_msg % oe.message
            sys.exit(err_msg)
        return sqoop_eval

    def eval(self, jdbc, sql_stmt, db_username, password_file):
        """
        :param jdbc: String jdbc url
        :param sql_stmt: String Sql statement to query against data source
        :param db_username: String Username for databases
        :param password_file: jceks#alias or password_file_path
        :return p: List Result set of query
        """
        # Get source from jdbc url
        src = self.get_jdbc_source(jdbc)
        driver = self.get_driver(jdbc)

        results = []

        if self.get_sqoop_eval():
            if 'jceks' in password_file:
                try:
                    jceks, password_alias = password_file.split('#')
                except ValueError as err_exp:
                    msg = ('Error unpacking jceks path and password alias '
                           'from {password}. '
                           'Expecting jceks:path#password_alias. Example: '
                           'jceks://hdfs/user/dev/fake.passwords.jceks#'
                           'fake.password.alias')
                    msg = msg.format(password=password_file)
                    self.logger.error(msg)
                    err_msg = ('Error found in sqoop_helper, exit from process'
                               ' with errors - reason {0}')
                    self.logger.error(err_msg.format(err_exp.message))
                    sys.exit(1)
                cmd_list = [
                    'sqoop-eval',
                    '-D hadoop.security.credential.provider.path=' + jceks,
                    '--driver', driver, '--verbose', '--connect', jdbc,
                    '--query', sql_stmt, '--username', db_username,
                    '--password-alias', password_alias]
            else:
                cmd_list = [
                    'sqoop-eval', '--driver', driver, '--verbose',
                    '--connect', jdbc, '--query', sql_stmt, '--username',
                    db_username, '--password-file', password_file]
            self.logger.info('Sqoop eval:  {stmt}'.format(stmt=sql_stmt))

            proc = subprocess.Popen(cmd_list, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            output, err = proc.communicate()
            if err:
                # sqoop prints debug messages to stderr even if query succeeds
                if 'updateCount=-1' not in err:
                    self.logger.error(output)
                    self.logger.error(err)
                    raise ValueError('Failed on sqoop eval!')
            re_expr = re.compile(r"\|.*\|")
            cleaned_lines = re_expr.findall(output)
            cleaned_lines = [line.split('|') for line in cleaned_lines]
            results = [[y.strip() for y in c] for c in cleaned_lines]
            # remove empty strings from result set
            results = [x[1:-1] for x in results[1:]]
        else:
            msg = 'Warning sqoop-eval not available'
            self.logger.warning(msg)
            sys.exit("Error: Command not available - reason %s" % msg)
        return results

    def _eval(self, jdbc, sql_stmt, db_username, password_file):
        """Fetch data for the query.
        Args:
            jdbc: JDBC Url
            sql_stmt: sql statement to query against data source
            db_username: username for source db
            password_file: jceks#alias or password_file_path
        Returns:
            returncode: 0 is success and anything else is failure
            output: sqoop output as it is
            err: sqoop error if any
        """
        src = self.get_jdbc_source(jdbc)
        driver = self.get_driver(jdbc)

        if 'jceks' in password_file:
            jceks, password_alias = password_file.split('#')
            cmd_list = ['sqoop-eval',
                        '-D hadoop.security.credential.provider.path=' + jceks,
                        '--driver', driver, '--verbose',
                        '--connect', jdbc, '--query', sql_stmt,
                        '--username', db_username, '--password-alias',
                        password_alias]
        else:
            cmd_list = ['sqoop-eval', '--driver', driver, '--verbose',
                        '--connect', jdbc, '--query', sql_stmt, '--username',
                        db_username, '--password-file', password_file]
        proc = subprocess.Popen(cmd_list, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        output, err = proc.communicate()
        return proc.returncode, output, err

    def get_driver(self, jdbc):
        """Given a jdbc url get driver type
        :param jdbc: String
        :return driver: String
        """
        driver = ''
        windows_auth = 'domain'
        for db_src in VALID_SOURCES:
            if db_src == SQLSERVER:
                if db_src in jdbc and windows_auth in jdbc:
                    driver = DRIVERS[db_src][1]
                else:
                    driver = DRIVERS[db_src][0]
            elif db_src in jdbc:
                driver = DRIVERS[db_src]

        return driver

    def get_jdbc_source(self, jdbc):
        """Given a jdbc url get database type from url
        :param jdbc: String
        :return src: String
        """
        src = ''
        for db_src in VALID_SOURCES:
            if db_src in jdbc:
                src = db_src
        return src

    def get_ddl_query(self, jdbc, database, tbl, schema=None):
        """Get ddl query"""
        db_type = self.get_jdbc_source(jdbc)
        if db_type == SQLSERVER:
            params = {'db': database, 'tbl': tbl, 'schema': schema}
        elif db_type == MYSQL:
            params = {'db': database, 'tbl': tbl}
        elif db_type == POSTGRESQL:
            params = {'db': database, 'tbl': tbl, 'schema': schema}
        else:
            params = {'db': database.upper(), 'tbl': tbl.upper()}
        columns_and_types = {
            'db2': ("SELECT NAME, COLTYPE FROM SYSIBM.SYSCOLUMNS"
                    " WHERE TBCREATOR='{db}' AND TBNAME='{tbl}'"),
            'sqlserver': ("SELECT COLUMN_NAME, DATA_TYPE FROM "
                          "INFORMATION_SCHEMA.COLUMNS WHERE "
                          "TABLE_CATALOG='{db}' AND TABLE_NAME='{tbl}' "
                          "AND TABLE_SCHEMA='{schema}'"),
            'oracle': ("SELECT COLUMN_NAME, DATA_TYPE FROM all_tab_columns"
                       " WHERE OWNER='{db}' AND TABLE_NAME='{tbl}'"),
            'teradata': "HELP COLUMN {db}.{tbl}.*;",
            'mysql': ("SELECT COLUMN_NAME, DATA_TYPE FROM "
                      "INFORMATION_SCHEMA.COLUMNS WHERE "
                      "TABLE_SCHEMA = '{db}' and TABLE_NAME = '{tbl}'"),
            'postgresql': ("SELECT COLUMN_NAME, DATA_TYPE FROM "
                           "INFORMATION_SCHEMA.COLUMNS WHERE "
                           "TABLE_CATALOG='{db}' AND TABLE_NAME='{tbl}' "
                           "AND TABLE_SCHEMA='{schema}'")}
        sql_stmt = columns_and_types[db_type]
        sql_stmt = sql_stmt.format(**params)
        return sql_stmt

    def get_ddl_table_view(self, jdbc, database, tbl):
        """Get ddl query"""
        db_type = self.get_jdbc_source(jdbc)
        params = {'db': database.upper(), 'tbl': tbl.upper()}
        columns_and_types = {
            'oracle': ("SELECT OBJECT_TYPE FROM ALL_OBJECTS"
                       " WHERE OWNER='{db}' AND OBJECT_NAME='{tbl}'")}
        sql_stmt = columns_and_types[db_type]
        sql_stmt = sql_stmt.format(**params)
        return sql_stmt

    def get_column_types(self, database, tbl, jdbc, db_username, password_file,
                         schema=None):
        """
        Args:
            database: String Name of database
            tbl: String Name of table
            jdbc: String JDBC url
            db_username: String Database username
            password_file: String path to password file
            schema: SQLSERVER schema
        Returns:
            List of columns types. [[name, data_type]]
        """
        global SQOOP_CACHE

        col_types = None
        info_msg = 'Getting column types for {db}.{tbl} with jdbc url {jdbc}'
        info_msg = info_msg.format(db=database, tbl=tbl, jdbc=jdbc)
        self.logger.info(info_msg)
        db_type = self.get_jdbc_source(jdbc)

        if db_type in VALID_SOURCES:
            sql_stmt = self.get_ddl_query(jdbc, database, tbl, schema)
            if sql_stmt in SQOOP_CACHE.keys():
                self.logger.info('Sqoop cache hit!')
                col_types = SQOOP_CACHE[sql_stmt]
            else:
                col_types = self.eval(jdbc, sql_stmt, db_username,
                                      password_file)
            if len(col_types) == 0:
                err_msg = ('Failed: Query returned zero rows: {0}\n'
                           'Source table definition is not found in '
                           'metadata table')
                err_msg = err_msg.format(sql_stmt)
                raise ValueError(err_msg)
        else:
            msg = 'Please provide a valid database source, {sources}'
            msg = msg.format(sources=VALID_SOURCES)
            self.logger.error(msg)
        return col_types

    def get_object_types(self, database, tbl, jdbc, db_username,
                         password_file):
        """
        Args:
            database: String Name of database
            tbl: String Name of table
            jdbc: String JDBC url
            db_username: String Database username
            password_file: String path to password file
            schema: SQLSERVER schema
        Returns:
            List of columns types [name, data_type]
        """
        global SQOOP_CACHE_VIEW

        object_types = None
        info_msg = 'Getting column types for {db}.{tbl} with jdbc url {jdbc}'
        info_msg = info_msg.format(db=database, tbl=tbl, jdbc=jdbc)
        self.logger.info(info_msg)
        db_type = self.get_jdbc_source(jdbc)

        if db_type in VALID_SOURCES:
            sql_stmt = self.get_ddl_table_view(jdbc, database, tbl)
            if sql_stmt in SQOOP_CACHE_VIEW.keys():
                self.logger.info('Sqoop cache hit!')
                object_types = SQOOP_CACHE_VIEW[sql_stmt]
            else:
                object_types = self.eval(jdbc, sql_stmt, db_username,
                                         password_file)
        else:
            msg = 'Please provide a valid database source, {sources}'
            msg = msg.format(sources=VALID_SOURCES)
            self.logger.error(msg)
        return object_types

    def column_has_special_chars(self, col_name):
        """Checks if column has non alphanumeric characters
        of if column starts with _ or by a numeric character
        """
        has_special_chars = True
        re_non_aplha = re.search(self.pattern_non_alphanumeric, col_name)
        re_underscore = re.search(self.pattern_underscore_at_start, col_name)
        re_numeric = re.search(self.pattern_numeric_at_start, col_name)
        if re_non_aplha is None and re_underscore is None \
                and re_numeric is None:
            has_special_chars = False
        return has_special_chars

    def convert_special_chars(self, col_name):
        """Converts all non alphanumeric and underscore characters to empty"""
        col_name = re.sub(self.pattern_non_alphanumeric, '', col_name)
        col_name = self.prefix_i_column_name(col_name)
        return col_name

    def prefix_i_column_name(self, col_name):
        """convert column name if it starts with numeric or underscore"""
        re_numeric = re.search(self.pattern_numeric_at_start, col_name)
        re_underscore = re.search(self.pattern_underscore_at_start, col_name)
        if re_numeric is not None:
            col_name = 'i_' + col_name
        if re_underscore is not None:
            col_name = 'i' + col_name
        return col_name

    def get_column_names(self, col_types):
        """Fetch all column names
        Args:
            col_types: List[column_name, column_type]
        Returns:
            List[column_name]
        """
        col_names = []
        for col in col_types:
            col_names.append(col[0])
        return col_names

    def get_timestamp_columns(self, col_types, jdbc_url):
        """Given a list of list[column name, column type], return a list of
        all columns being casted to timestamp
        Args:
            col_types: List[List[String, String]] List of list containing
                       column name and column type
        Returns:
            ts_cols: List[String]
        """
        global SQLSERVER

        sources = {
            # teradata
            "AT": "TIMESTAMP",
            "DA": "TIMESTAMP",
            "TS": "TIMESTAMP",
            # oracle
            "DATE": "TIMESTAMP",
            "TIMESTAMP": "TIMESTAMP",
            "TIMESTAMP WITH TIME ZONE": "TIMESTAMP",
            "TIMESTAMP WITH LOCAL TIME ZONE": "TIMESTAMP",
            "INTERVAL YEAR TO MONTH": "TIMESTAMP",
            "INTERVAL DAY TO SECOND": "TIMESTAMP",
            "TIMESTAMP(6)": "TIMESTAMP",
            "TIMESTAMP(6) WITH TIME ZONE": "TIMESTAMP",
            # db2 specific in SysColumns
            "TIMESTMP": "TIMESTAMP",
            # SQL SERVER
            "DATETIME": "TIMESTAMP",
            # MYSQL
            "TIME": "TIMESTAMP",
            "YEAR": "TIMESTAMP",
        }
        ts_cols = []
        for col in col_types:
            col_name = col[0]
            col_type = col[1]
            if sources.get(col_type.upper(), '') == 'TIMESTAMP':
                col_name = self.convert_special_chars(col_name)
                ts_cols.append(col_name)
        return ts_cols

    def get_lob_columns(self, col_types, jdbc_url):
        """Given a list of list[column name, column type], return a list of
        all columns being casted to LOB (Large Objects)
        :param col_types: List[List[String, String]] List of list containing
                          column name and column type
        :return lob_cols: List[String]
        """
        sources = ["CLOB", "DBCLOB", "BLOB", "NCLOB", "TEXT", "NTEXT", "IMAGE",
                   "FILESTREAM", "BFILE", "XMLTYPE", "LONG", "LONG RAW", "RAW",
                   "UROWID", "ROWID", "URITYPE", "BINARY_FLOAT",
                   "BINARY_DOUBLE", "OID", "ARRAY", "XID", "MACADDR", "UUID",
                   "NAME", "REGPROC", "PG_NODE_TREE", "PG_LSN", "ANYARRAY",
                   "BOX", "BIGSERIAL", "BYTEA", "CHARACTER", "CIDR", "CIRCLE",
                   "JSON", "JSONB", "LINE", "LSEG", "POINT", "POLYGON", "REAL",
                   "SMALLSERIAL", "SERIAL", "TSQUERY", "TSVECTOR",
                   "TXID_SNAPSHOT"]

        lob_cols = []
        for col in col_types:
            col_name = col[0]
            col_type = col[1]
            if col_type.upper() in sources:
                col_name = self.convert_special_chars(col_name)
                lob_cols.append(col_name)
        return lob_cols
