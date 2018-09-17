"""Generate hql file."""
import ConfigParser
import os
import re
import sys
import datetime
from sqoop_utils import SqoopUtils
from sql_queries import ORACLE, DB2, TD, SQLSERVER, MYSQL, POSTGRESQL
from impala_utils import ImpalaConnect

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class DDLTypes(object):

    """
    Class containing all the necessary functions to collect
    the actual data types from a given source system and do any
    needed casting.
    """

    def __init__(self, input_mapping, data_source, ingest_timestamp):
        self.input_mapping = input_mapping
        self.data_source = data_source
        self.ingest_time = ingest_timestamp

        # TODO: Remove this out of the init function
        self.select_hql, self.create_hql = self.create_ddl_mappings()

    def get_select_hql(self):
        return self.select_hql

    def get_create_hql(self):
        return self.create_hql

    def get_types_schema(self, checking_column):
        """Return data type info for table creation"""
        sources = {
            "A1": "STRING",
            "AN": "STRING",
            "AT": "TIMESTAMP",
            "BF": "STRING",
            "BO": "STRING",
            "BV": "STRING",
            "CF": "CHAR",
            "CO": "STRING",
            "CV": "VARCHAR",
            "D": "DECIMAL",
            "DA": "TIMESTAMP",
            "DH": "STRING",
            "DM": "STRING",
            "DS": "STRING",
            "DY": "STRING",
            "F": "DOUBLE",
            "HM": "STRING",
            "HS": "STRING",
            "HR": "STRING",
            "I": "INT",
            "I1": "INT",
            "I2": "SMALLINT",
            "I8": "BIGINT",
            "JN": "STRING",
            "MI": "STRING",
            "MO": "STRING",
            "MS": "STRING",
            "N": "INT",
            "PD": "STRING",
            "PM": "STRING",
            "PS": "STRING",
            "PT": "STRING",
            "PZ": "STRING",
            "SC": "STRING",
            "SZ": "STRING",
            "TS": "TIMESTAMP",
            "TZ": "STRING",
            "UT": "STRING",
            "XM": "STRING",
            "YM": "STRING",
            "YR": "STRING",
            "++": "STRING",
            # special
            "TIMESTAMP_DATE": "TIMESTAMP_DATE",
            # oracle
            "NVARCHAR2": "VARCHAR",
            "VARCHAR2": "VARCHAR",
            "VARCHAR": "VARCHAR",
            "CHAR": "CHAR",
            "TIMESTAMP": "TIMESTAMP",
            "DATE": "TIMESTAMP_DATE",
            "NUMBER": "DECIMAL",
            "DECIMAL": "DECIMAL",
            "SMALLINT": "SMALLINT",
            "BIGINT": "BIGINT",
            "INT": "INT",
            "INTEGER": "INT",
            "FLOAT": "FLOAT",
            "BINARY_FLOAT": "STRING",
            "BINARY_DOUBLE": "STRING",
            "TIMESTAMP(6) WITH TIME ZONE": "STRING",
            "TIMESTAMP(6) WITH LOCAL TIME ZONE": "STRING",
            "INTERVAL": "STRING",
            "INTERVAL YEAR(2) TO MONTH": "STRING",
            "INTERVAL DAY(2) TO SECOND(6)": "STRING",
            # also 'REAL' impala supports
            "DOUBLE": "DOUBLE",
            "TINYINT": "TINYINT",
            "BOOLEAN": "BOOLEAN",
            "STRING": "STRING",
            "NUMERIC": "STRING",
            "BIT": "INT",
            # db2 specific in SysColumns
            "TIMESTMP": "TIMESTAMP",
            "TIME": "STRING",
            "GRAPHIC": "STRING",
            "VARGRAPHIC": "STRING",
            "LONG VARCHAR": "STRING",
            "LONG VARGRAPHIC": "STRING",
            "DECFLOAT": "STRING",
            "BINARY": "STRING",
            "VARBINARY": "STRING",
            "REAL": "STRING",
            "XML": "STRING",
            "TIMESTAMP WITH TIME ZONE": "STRING",
            "TIMESTAMP WITHOUT TIME ZONE": "STRING",
            # SQL SERVER
            "DATETIME": "TIMESTAMP",
            "DATETIME2": "TIMESTAMP",
            "SMALLDATETIME": "TIMESTAMP",
            "DATETIMEOFFSET": "STRING",
            "UNIQUEIDENTIFIER": "STRING",
            "MONEY": "STRING",
            "SMALLMONEY": "STRING",
            "TEXT": "STRING",
            "NTEXT": "STRING",
            "IMAGE": "STRING",
            "CURSOR": "STRING",
            "HIERARCHYID": "STRING",
            "SQL_VARIANT": "STRING",
            "TABLE": "STRING",
            "GEOGRAPHY": "STRING",
            "GEOMETRY": "STRING",
            # handles unicode, VARCHAR only ASCII
            "NVARCHAR": "STRING",
            "NCHAR": "STRING",
            "BLOB": "STRING",
            "CLOB": "STRING",
            "DBCLOB": "STRING",
            "NCLOB": "STRING",
            "BFILE": "STRING",
            "RAW": "STRING",
            "LONG": "STRING",
            "LONG RAW": "STRING",
            "ROWID": "STRING",
            "UROWID": "STRING",
            "XMLType": "STRING",
            "URITYPE": "STRING",
            # MySQL
            "BOOL": "BOOLEAN",
            "MEDIUMINT": "INT",
            "DEC": "DECIMAL",
            "DOUBLE PRECISION": "DOUBLE",
            "YEAR": "STRING",
            "LONGTEXT": "STRING",
            "LONGBLOB": "STRING",
            "MEDIUMTEXT": "STRING",
            "MEDIUMBLOB": "STRING",
            "TINYTEXT": "STRING",
            "TINYBLOB": "STRING",
            # Postgres
            "OID": "STRING",
            "ARRAY": "STRING",
            "XID": "STRING",
            "MACADDR": "STRING",
            "UUID": "STRING",
            "REGPROC": "STRING",
            "PG_NODE_TREE": "STRING",
            "PG_LSN": "STRING",
            "ANYARRAY": "STRING",
            "NAME": "STRING",
            "BOX": "STRING",
            "BIGSERIAL": "STRING",
            "BYTEA": "STRING",
            "CHARACTER": "STRING",
            "CIDR": "STRING",
            "CIRCLE": "STRING",
            "JSON": "STRING",
            "JSONB": "STRING",
            "LINE": "STRING",
            "LSEG": "STRING",
            "POINT": "STRING",
            "POLYGON": "STRING",
            "SMALLSERIAL": "STRING",
            "SERIAL": "STRING",
            "TSQUERY": "STRING",
            "TSVECTOR": "STRING",
            "TXID_SNAPSHOT": "STRING"
        }
        return sources[checking_column]

    @property
    def is_oracle(self):
        """test if oracle"""
        return bool(self.data_source == 'oracle')

    @property
    def is_db2(self):
        """test if db2"""
        return bool(self.data_source == 'db2')

    @property
    def is_td(self):
        """test if teradata"""
        return bool(self.data_source == 'td')

    @property
    def is_sqlserver(self):
        """test if sql server"""
        return bool(self.data_source == 'sqlserver')

    @property
    def is_postgresql(self):
        """test if postgresql"""
        return bool(self.data_source == 'postgresql')

    @property
    def is_mysql(self):
        """test if my sql"""
        return bool(self.data_source == 'mysql')

    def convert_spl_chars(self, column_name):
        """Converts special characters to empty str in column name"""
        pattern_non_alphanumeric = re.compile(r'[^A-Za-z0-9_]')
        pattern_numeric_at_beginning = r'^\d'
        _name = re.sub(pattern_non_alphanumeric, '', column_name)
        if re.search(pattern_numeric_at_beginning, _name) is not None:
            _name = 'i_' + _name
        return _name

    def decimal_name_type(self, row):
        """Returns column name and type definition"""

        if self.is_oracle:
            if row[4] == '(null)':
                precision_scale = '38,0'
            else:
                precision_scale = row[4] + "," + row[5]
        elif self.is_db2:
            precision_scale = row[3] + "," + row[4]
        elif self.is_td:
            precision_scale = row[6] + "," + row[7]
        elif self.is_sqlserver:
            precision_scale = row[4] + "," + row[5]
        elif self.is_postgresql:
            precision_scale = row[4] + "," + row[5]
        elif self.is_mysql:
            precision_scale = row[4] + "," + row[5]
        type_def = "DECIMAL(" + precision_scale + ")"
        return self.convert_spl_chars(row[1]), type_def

    def decimal_t(self, row):
        """Decimal cast"""
        column_name, column_type = self.decimal_name_type(row)
        # in all_tab_columns, Number is stored as 22. Need the below
        # additional fields to get accurate size. Checks to
        # see if is null, and defaults to 22.
        val = self.format_select(column_name, column_type)
        return val

    def varchar_name_type(self, row):
        """Returns column name and type definition"""
        if self.is_oracle:
            char_length = row[7]
        elif self.is_db2:
            char_length = row[3]
        elif self.is_td:
            char_length = row[5]
        elif self.is_sqlserver:
            if row[3] != '-1':
                char_length = row[3]
            else:
                char_length = '1000'
        elif self.is_postgresql:
            if row[3] != '-1':
                char_length = row[3]
            else:
                char_length = '1000'
        elif self.is_mysql:
            char_length = row[3]
        type_def = "VARCHAR(" + char_length + ")"
        return self.convert_spl_chars(row[1]), type_def

    def varchar_t(self, row):
        """varchar cast"""
        column_name, column_type = self.varchar_name_type(row)
        val = self.format_select(column_name, column_type)
        return val

    def char_name_type(self, row):
        """Returns column name and type definition"""
        if self.is_oracle:
            char_length = row[7]
        elif self.is_db2:
            char_length = row[3]
        elif self.is_td:
            char_length = row[5]
        elif self.is_sqlserver:
            char_length = row[3]
        elif self.is_postgresql:
            char_length = row[3]
        elif self.is_mysql:
            char_length = row[3]
        type_def = "CHAR(" + char_length + ")"
        return self.convert_spl_chars(row[1]), type_def

    def char_t(self, row):
        """char cast"""
        column_name, column_type = self.char_name_type(row)
        val = self.format_select(column_name, column_type)
        return val

    def timestamp_name_type(self, row):
        """Returns column name and type definition"""
        return self.convert_spl_chars(row[1]), "TIMESTAMP"

    def timestamp_t(self, row):
        """timestamp cast"""
        column_name, column_type = self.timestamp_name_type(row)
        val = self.format_select(column_name, column_type)
        return val

    def timestamp_date_name_type(self, row):
        """Returns column name and type definition"""
        return self.convert_spl_chars(row[1]), "TIMESTAMP"

    def timestamp_date(self, row):
        """timestamp-date cast"""
        column_name, column_type = self.timestamp_date_name_type(row)
        timestamp_format = 'yyyy-MM-dd'
        options = {'oracle': 'yyyy-MM-dd HH:mm:ss'}
        if self.data_source in options:
            timestamp_format = options[self.data_source]
        val = self.format_select(column_name, column_type,
                                 timestamp_format=timestamp_format)
        return val

    def remove_pipes_from_input(self, noise_and_pipes_input):
        """
        The input contains pretty-print text, surrounded by pipes.
        :return: only the text containing pipes, not the surrounding "noise"
        """
        regex = re.compile(r"\|.*\|")
        matched = regex.findall(noise_and_pipes_input)
        return matched

    def remove_6_in_timestamp_columns(self, column_type):
        """
        Needed for removing (6) in TIMESTAMP columns
        :return:(6) removed, if it exists
        """
        regex = re.compile("[A-Za-z0-9]+")
        fixed_column = regex.findall(column_type)
        return fixed_column

    def remove_pipes_from_hql(self, piped_input):
        """
        Take in input that is seperated by pipes (from a sqoop eval)
        and parse out the pipes to provide rows that can be dealt with
        individually
        :param input: pipe-seperated input
        :return: list: containing lines of text
        """
        return [[element.strip() for element in row] for row in
                [line.split('|') for line in piped_input]]

    def func_of_all_special_types(self):
        """
        Dictionary set up of all special functions
        :return: Dictionary containing pointers to special-type functions
        """
        type_functs = dict()
        type_functs['DECIMAL'] = (self.decimal_t, self.decimal_name_type)
        type_functs['VARCHAR'] = (self.varchar_t, self.varchar_name_type)
        type_functs['CHAR'] = (self.char_t, self.char_name_type)
        type_functs['TIMESTAMP'] = (self.timestamp_t, self.timestamp_name_type)
        type_functs['TIMESTAMP_DATE'] = (self.timestamp_date,
                                         self.timestamp_date_name_type)

        return type_functs

    def format_select(self, col_name, data_type, timestamp_format=None):
        """
        1. Avro adds an additional underscore if the column name already
        starts with one. We have to add one at this step to "trick" the
        select statement to be able to select from the ingest.avro_table
        as well as ensure that the data returns back to normal

        2. If the column has spaces, Avro will ingest w/o the spaces.
        In this case we need to remove the spaces. Also,
        Hive does not want spaces in column names so we make sure
        that it's all non-spaced.

        :param col_name: string: the name of the column
        :param data_type: string: data type of the column
        :return: Formatted select cast, fixing issue with avro if necessary
        """
        final_cast_string = "CAST({0} AS {1}) AS `{2}`"

        col_name = self.convert_spl_chars(col_name)

        if col_name.startswith('_'):
            col_name_avro = 'i' + col_name
        elif col_name == 'public':
            col_name_avro = '_' + col_name
        else:
            col_name_avro = col_name

        if timestamp_format:
            # "CAST(from_unixtime(unix_timestamp(`__col`, 'yyyy-mm-dd'))
            # AS INT) AS `_col`"
            return final_cast_string.format(
                "from_unixtime(unix_timestamp(`{0}`, '{1}'))".format(
                    col_name_avro, timestamp_format), data_type, col_name)
        else:
            # CAST(`__col` AS INT AS `_col`)
            return final_cast_string.format('`' + col_name_avro + '`',
                                            data_type, col_name)

    def create_ddl_mappings(self):
        type_functs = self.func_of_all_special_types()
        matched = self.remove_pipes_from_input(self.input_mapping)
        parsed_hql = self.remove_pipes_from_hql(matched)

        select_hql_lst = []
        create_hql_lst = []
        for row in parsed_hql[1:]:
            col_type = row[2]
            stripped = self.remove_6_in_timestamp_columns(col_type)
            # get the corresponding value - eg. NUMBER -> DECIMAL
            data_type = self.get_types_schema(stripped[0].upper())
            if data_type in type_functs.keys():
                # call the corresponding function
                type_func = type_functs[data_type][0]
                select_hql_lst.append(type_func(row))
                create_type_func = type_functs[data_type][1]
                col_name, col_type = create_type_func(row)
                create_hql_lst.append("`{0}` {1}".format(col_name, col_type))
            else:
                row[1] = self.convert_spl_chars(row[1])
                val = self.format_select(row[1], data_type)
                select_hql_lst.append(val)
                create_hql_lst.append("`{0}` {1}".format(row[1], data_type))

        select_hql = ",\n ".join(select_hql_lst)
        select_hql += ",\n '" + self.ingest_time + "' AS `ingest_timestamp`"
        create_hql = ",\n ".join(create_hql_lst)

        return select_hql, create_hql


class ConnectionManager(object):

    """Connection manager."""

    def __init__(
            self,
            database,
            table,
            schema_name,
            domain,
            jdbc_url,
            connection_factories,
            db_username,
            password_file,
            views,
            ibis_env,
            domains,
            impala_host,
            ingest_time,
            target_dir,
            jars,
            hive2_jdbc_url,
            queue_name):
        """Init."""
        self.source = None
        self.queue_name = queue_name
        self.hive2_jdbc_url = hive2_jdbc_url
        self.jdbc_url = jdbc_url
        self.database = database
        self.table_name = table
        self.clean_database = ''
        self.clean_table_name = ''
        self.ingest_tbl = ''
        self.avsc_file = ''
        self.jars = jars
        self.ingest_time = ingest_time
        ingest_dt = datetime.datetime.strptime(
            self.ingest_time, '%Y-%m-%d %H:%M:%S')
        self.partition_name = ingest_dt.strftime('%Y%m%d%H%M%S')
        self.target_dir = target_dir
        self.domain = domain
        self.connection_factories = connection_factories
        self.username = db_username
        self.password = password_file
        self.view = views
        self.schema = schema_name
        self.sqoop_obj = None

        self.ddl_types = self.get_schema()
        self.set_names()
        self.ibis_env = ibis_env
        self.domain_list = domains
        self.host_name = impala_host
        self.freq_ingest = 'ibis.freq_ingest'

    def set_names(self):
        """sets up table names that are valid without special chars"""
        self.clean_database = self.convert_spl_chars(self.database)
        self.clean_table_name = self.convert_spl_chars(self.table_name)
        self.ingest_tbl = 'ingest.{database}_{table_name}'
        self.ingest_tbl = self.ingest_tbl.format(
            database=self.clean_database, table_name=self.clean_table_name)
        self.parquet_stage_tbl = 'parquet_stage.{database}_{table_name}'
        self.parquet_stage_tbl = self.parquet_stage_tbl.format(
            database=self.clean_database, table_name=self.clean_table_name)
        self.final_tbl = '{domain}.{database}_{table_name}'.format(
            domain=self.domain, database=self.clean_database,
            table_name=self.clean_table_name)
        self.avsc_file = '{0}.avsc'.format(self.table_name)

    def convert_spl_chars(self, value):
        """convert spl chars to empty"""
        pattern_non_alphanumeric = re.compile(r'[^A-Za-z0-9_]')
        value = re.sub(pattern_non_alphanumeric, '', value)
        return value

    def build_ddl_query(self):
        """Determine db vendor. Column ordering is added Oracle & DB2, as the
        metadata tables do not always maintain the column ordering
        that is required for the Avro ingest
        """
        if 'teradata' in self.jdbc_url:
            self.source = 'td'
            db_query = TD['ddl']
            db_query = db_query.format(table_name=self.table_name.upper())
        elif 'oracle' in self.jdbc_url:
            self.source = 'oracle'
            db_query = ORACLE['ddl']
            db_query = db_query.format(database_name=self.database.upper(),
                                       table_name=self.table_name.upper())
        elif 'db2' in self.jdbc_url:
            self.source = 'db2'
            db_query = DB2['ddl']
            db_query = db_query.format(database_name=self.database.upper(),
                                       table_name=self.table_name.upper())
        elif 'sqlserver' in self.jdbc_url:
            self.source = 'sqlserver'
            db_query = SQLSERVER['ddl']
            db_query = db_query.format(
                database_name=self.database, table_name=self.table_name,
                schema_name=self.schema)
        elif 'postgresql' in self.jdbc_url:
            self.source = 'postgresql'
            db_query = POSTGRESQL['ddl']
            db_query = db_query.format(
                database_name=self.database, table_name=self.table_name,
                schema_name=self.schema)
        elif 'mysql' in self.jdbc_url:
            self.source = 'mysql'
            db_query = MYSQL['ddl']
            db_query = db_query.format(database_name=self.database,
                                       table_name=self.table_name)
        else:
            db_query = None
            self.source = None
        return db_query

    def sqoop_eval(self, query):
        """Run sqoop eval."""
        if 'jceks' in self.password:
            jceks, password = self.password.split('#')
        else:
            jceks = 'None'
            password = self.password

        self.sqoop_obj = SqoopUtils(self.jars, self.jdbc_url,
                                    self.connection_factories, self.username,
                                    jceks, password)

        returncode, output, err = self.sqoop_obj.eval(query)

        if returncode != 0:
            print '-' * 100
            print query
            print '-' * 100
            print err
            print '-' * 100
            print output
            print '-' * 100
            raise ValueError(err)

        return output

    def get_schema(self):
        """Return schema hql"""
        query = self.build_ddl_query()
        output = self.sqoop_eval(query)
        _, col_rows = self.sqoop_obj.fetch_rows_sqoop(output)
        if not col_rows:
            print query
            print output
            err_msg = 'Failed - Query returned empty: {0}'.format(query)
            raise ValueError(err_msg)
        ddl_types = DDLTypes(output, self.source, self.ingest_time)

        return ddl_types

    def create_ingest_table(self):
        """Create ingest table"""
        hql = (
            "SET mapred.job.queue.name={queue_name};\n"
            "set hive.warehouse.subdir.inherit.perms=true;\n\n"
            "create database if not exists `ingest`;\n\n"
            "drop table if exists `{ingest_tbl}`;\n\n"
            "create external table `{ingest_tbl}` "
            "row format serde "
            "'org.apache.hadoop.hive.serde2.avro.AvroSerDe' "
            "STORED AS INPUTFORMAT "
            "'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' "
            "OUTPUTFORMAT "
            "'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' "
            "location 'hdfs:///user/data/ingest/{target_dir}' "
            "tblproperties ('avro.schema.url'='hdfs:///user/data/ingest"
            "/{target_dir}/_gen/{avsc_file}');\n\n")
        hql = hql.format(queue_name=self.queue_name,
                         ingest_tbl=self.ingest_tbl,
                         target_dir=self.target_dir,
                         avsc_file=self.avsc_file)
        return hql

    def create_parquet_live(self):
        """create parquet_live.hql to throw the schema over
        the moved parquet files and drop the temp table
        """
        hql = (
            "SET mapred.job.queue.name={queue_name};\n"
            "SET mapreduce.map.memory.mb=8000;\n"
            "SET mapreduce.reduce.memory.mb=16000;\n"
            "set hive.warehouse.subdir.inherit.perms=true;\n\n"
            "create database if not exists `{domain}`;\n\n"
            "drop table if exists `{final_tbl}`;\n\n"
            "create external table `{final_tbl}` "
            "like `{parquet_stage_tbl}` "
            "location 'hdfs:///user/data/{target_dir}/live/';\n\n"
            "alter table `{final_tbl}` add partition "
            "(incr_ingest_timestamp='full_{partition_name}');\n\n"
            "drop table if exists `{parquet_stage_tbl}`;\n"
            "drop table if exists `{ingest_tbl}`;\n")
        hql = hql.format(queue_name=self.queue_name, domain=self.domain,
                         final_tbl=self.final_tbl,
                         parquet_stage_tbl=self.parquet_stage_tbl,
                         partition_name=self.partition_name,
                         target_dir=self.target_dir,
                         ingest_tbl=self.ingest_tbl)
        return hql

    def hive_default_configs(self):
        """Default Hive config options needed at the top of all HQL files"""
        hive_options = (
            'SET mapred.job.queue.name={0};\n'
            'SET mapreduce.map.memory.mb=8000;\n'
            'SET mapreduce.reduce.memory.mb=16000;\n'
            'SET hive.exec.dynamic.partition.mode=nonstrict;\n'
            'SET hive.exec.dynamic.partition=true;\n'
            'SET hive.warehouse.subdir.inherit.perms=true;\n\n'
            "SET parquet.block.size=268435456; \n"
            "SET dfs.blocksize=1000000000; \n"
            "SET mapred.min.split.size=1000000000; \n\n")
        hive_options = hive_options.format(self.queue_name)
        return hive_options

    def construct_hql(self, select_hql, create_hql, rpc_method):
        """Construct parquet stage table hql."""
        if rpc_method == "incremental":
            incr_ingest_timestamp = self.partition_name
        else:
            incr_ingest_timestamp = 'full_{0}'.format(self.partition_name)

        #  We are partitioning this table because we just "rename" this table
        #  after it's been loaded, to the final table.
        # This isn't necessarily needed for incremental
        hql = self.hive_default_configs()
        hql += (
            "CREATE DATABASE IF NOT EXISTS `parquet_stage`;\n\n"
            "DROP TABLE IF EXISTS `{parquet_stage_tbl}`;\n\n"
            "CREATE TABLE `{parquet_stage_tbl}`(\n"
            "{create_column_hql},\n`ingest_timestamp` STRING)\n"
            "PARTITIONED BY (incr_ingest_timestamp STRING)\n"
            "STORED AS PARQUET LOCATION "
            "'hdfs:///user/data/{target_dir}/stage/';\n\n"
            "msck repair table `{parquet_stage_tbl}`;\n\n"
            "INSERT OVERWRITE TABLE `{parquet_stage_tbl}`"
            " partition(incr_ingest_timestamp='{partition_name}') "
            "SELECT {select_column_hql}\n FROM `{ingest_tbl}`;\n\n"
            "DROP TABLE `{ingest_tbl}`;\n\n")
        hql = hql.format(parquet_stage_tbl=self.parquet_stage_tbl,
                         create_column_hql=create_hql,
                         select_column_hql=select_hql,
                         target_dir=self.target_dir,
                         partition_name=incr_ingest_timestamp,
                         ingest_tbl=self.ingest_tbl)
        return hql

    def get_hql(self, rpc_method="full_"):
        """Wrapper for building hql"""
        select_hql = self.ddl_types.get_select_hql()
        create_hql = self.ddl_types.get_create_hql()

        hql = self.construct_hql(select_hql, create_hql, rpc_method)
        return hql

    def get_incremental_hql(self):
        """Wrapper for building incremental ingestion hql"""
        select_column_hql = self.ddl_types.get_select_hql()
        merge_table_hql = self.hive_default_configs()
        merge_table_hql += (
            "INSERT OVERWRITE TABLE `{final_tbl}` "
            "PARTITION(incr_ingest_timestamp) "
            "SELECT {select_column_hql},\n"
            "'{partition_name}' AS `incr_ingest_timestamp` "
            "FROM `{parquet_stage_tbl}`;\n")
        merge_table_hql = merge_table_hql.format(
            final_tbl=self.final_tbl,
            select_column_hql=select_column_hql,
            partition_name=self.partition_name,
            parquet_stage_tbl=self.parquet_stage_tbl)
        return merge_table_hql

    def create_externaltable(self, rpc_method):
        """Wrapper for building create external table for views"""
        create_view_hql = impala_invalidate = views_info = ''
        create_hql = self.ddl_types.get_create_hql()
        impala_invalidate = 'invalidate metadata {final_tbl};\n'
        impala_invalidate = impala_invalidate.format(final_tbl=self.final_tbl)
        views_info = '{0} {1}_{2}\n'.format(self.domain, self.clean_database,
                                            self.clean_table_name)

        def gen_view_hql(target_db, domain=None):
            """creates view hql"""
            if domain is not None:
                team_view_name = '{team_nm}.{team_db}_{team_table}'
                team_view_name = team_view_name.format(
                    team_nm=domain, team_db=self.clean_database,
                    team_table=self.clean_table_name)

            view_full_name = '{view_name}.{database}_{table_name}'
            view_full_name = view_full_name.format(
                view_name=target_db, database=self.clean_database,
                table_name=self.clean_table_name)
            src_view = '{src_vw_name}.{src_db}_{src_tbl_name}'
            src_view = src_view.format(
                src_vw_name=self.domain, src_db=self.clean_database,
                src_tbl_name=self.clean_table_name)

            phi_domain = target_db.split('_non_phi')[0]
            only_domain = self.domain_list
            only_domain = only_domain.split(",")
            only_domain = set(only_domain).intersection([phi_domain])
            only_domain = list(only_domain)

            if rpc_method == 'incremental':
                add_partition_hql = ''
            elif rpc_method == 'full_load':
                add_partition_hql = (
                    "ALTER TABLE `{view_full_name}` ADD PARTITION "
                    "(incr_ingest_timestamp='full_{ingest_date}');\n\n")
                add_partition_hql = add_partition_hql.format(
                    view_full_name=view_full_name,
                    ingest_date=self.partition_name)

            if self.ibis_env == 'PERF' and len(only_domain) < 1:
                try:
                    try:
                        ImpalaConnect.run_query(
                            self.host_name, view_full_name,
                            'USE {db}'.format(db=view_full_name.split(".")[0]),
                            'create')
                        is_table_available = ImpalaConnect.run_query(
                            self.host_name, view_full_name,
                            "SHOW TABLES LIKE '{table_name}'".format(
                                table_name=view_full_name.split(".")[1]))

                        if is_table_available is None or \
                                is_table_available is '':
                            raise ValueError("Table not found")
                    except:
                        raise ValueError("Database/Table not found")

                    max_trgt_ingest = ('select max(ingest_timestamp) from '
                                       '{view_full_name}')
                    max_trgt_ingest = max_trgt_ingest.format(
                        view_full_name=view_full_name)
                    max_trgt_ingest = ImpalaConnect.run_query(
                        self.host_name, view_full_name,
                        max_trgt_ingest)

                    team_run_freq = ("select * from {freq_ingest} where"
                                     " view_name='{view_nm_splt}' and "
                                     "full_table_name='{table_name}'")
                    team_run_freq = team_run_freq.format(
                        view_nm_splt=target_db,
                        table_name=src_view, freq_ingest=self.freq_ingest)
                    team_run_freq = ImpalaConnect.run_query(
                        self.host_name, self.freq_ingest,
                        team_run_freq)
                    frequency = team_run_freq[0][0]

                    if frequency == 'none':
                        team_freq = 0
                    elif frequency == 'daily':
                        team_freq = 1
                    elif frequency == 'weekly':
                        team_freq = 7
                    elif frequency == 'biweekly':
                        team_freq = 14
                    elif frequency == 'fortnightly':
                        team_freq = 15
                    elif frequency == 'monthly':
                        team_freq = 30
                    elif frequency == 'quarterly':
                        team_freq = 90
                    elif frequency == 'yearly':
                        team_freq = 364

                    max_ingest = datetime.datetime.strptime(
                        max_trgt_ingest[0][0], '%Y-%m-%d %H:%M:%S')
                    curr_date_str = datetime.datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S")
                    curr_date = datetime.datetime.strptime(
                        curr_date_str, '%Y-%m-%d %H:%M:%S')
                    last_ingest_day = (curr_date - max_ingest).days
                    if team_freq != 0 and last_ingest_day >= team_freq and \
                            team_run_freq[0][1].lower() == 'yes':
                        if domain:
                            select_query = "select * from {team_view_name}".\
                                format(team_view_name=team_view_name)
                            views_hql = (
                                'DROP VIEW IF EXISTS {view_full_name};\n'
                                'CREATE DATABASE IF NOT EXISTS {view_name};'
                                '\n'
                                'CREATE DATABASE IF NOT EXISTS {team_name};'
                                '\n'
                                'DROP VIEW IF EXISTS {team_view_name};\n'
                                'DROP TABLE IF EXISTS {team_view_name};\n'
                                'CREATE VIEW  {team_view_name} AS '
                                'SELECT * FROM {src_view}; \n'
                                'CREATE TABLE IF NOT EXISTS {view_full_name} '
                                'like {src_view};\n\n')
                            views_hql = views_hql.format(
                                view_full_name=view_full_name,
                                view_name=target_db,
                                src_view=src_view,
                                team_view_name=team_view_name,
                                team_name=domain)

                        else:
                            select_query = "select * from {src_view}".format(
                                src_view=src_view)
                            views_hql = (
                                'DROP VIEW IF EXISTS {view_full_name};\n'
                                'CREATE DATABASE IF NOT EXISTS {view_name};'
                                '\n'
                                'CREATE  TABLE IF NOT EXISTS {view_full_name} '
                                'like {src_view};\n\n'
                                'INSERT OVERWRITE TABLE {view_full_name} '
                                'PARTITION(incr_ingest_timestamp) '
                                'select * from {src_view} ;\n\n')

                            views_hql = views_hql.format(
                                view_full_name=view_full_name,
                                view_name=target_db,
                                src_view=src_view)

                        if rpc_method == 'incremental':
                            insert_statement = (
                                "INSERT OVERWRITE TABLE {view_full_name} "
                                "PARTITION(incr_ingest_timestamp) "
                                "{select_query} where "
                                "ingest_timestamp > '{maxtime}';\n\n")
                            insert_statement = insert_statement.format(
                                view_full_name=view_full_name,
                                maxtime=max_trgt_ingest[0][0],
                                select_query=select_query)
                        else:
                            insert_statement = (
                                'INSERT OVERWRITE TABLE {view_full_name} '
                                'PARTITION(incr_ingest_timestamp) '
                                '{select_query};\n\n')
                            insert_statement = insert_statement.format(
                                view_full_name=view_full_name,
                                select_query=select_query)

                            drop_hql = (
                                'DROP TABLE IF EXISTS {view_full_name};\n\n')

                            views_hql = drop_hql.format(
                                view_full_name=view_full_name) + views_hql
                        views_hql += insert_statement

                        views_hql += "msck repair table {0};\n\n".format(
                            view_full_name)

                    else:
                        print 'No views hql created'
                        views_hql = ''

                except ValueError as ex:
                    print str(ex)
                    if domain:
                        views_hql = (
                            'DROP VIEW IF EXISTS {view_full_name};\n'
                            'DROP TABLE IF EXISTS {view_full_name};\n\n'
                            'CREATE DATABASE IF NOT EXISTS {view_name};\n'
                            'CREATE DATABASE IF NOT EXISTS {team_name};\n'
                            'DROP VIEW IF EXISTS {team_view_name};\n'
                            'DROP TABLE IF EXISTS {team_view_name};\n'
                            'CREATE VIEW  {team_view_name} AS '
                            'SELECT * FROM {src_view}; \n'
                            'CREATE  TABLE {view_full_name} '
                            'like {src_view};\n\n'
                            'INSERT OVERWRITE TABLE {view_full_name} '
                            'PARTITION(incr_ingest_timestamp) '
                            'select * from {team_view_name} ;\n\n'
                        )
                        views_hql = views_hql.format(
                            view_full_name=view_full_name,
                            view_name=target_db,
                            src_view=src_view,
                            team_view_name=team_view_name,
                            team_name=domain)
                    else:
                        views_hql = (
                            'DROP VIEW IF EXISTS {view_full_name};\n'
                            'DROP TABLE IF EXISTS {view_full_name};\n\n'
                            'CREATE DATABASE IF NOT EXISTS {view_name};\n'
                            'CREATE  TABLE {view_full_name} '
                            'like {src_view};\n\n'
                            'INSERT OVERWRITE TABLE {view_full_name} '
                            'PARTITION(incr_ingest_timestamp) '
                            'select * from {src_view} ;\n\n'
                        )

                        views_hql = views_hql.format(
                            view_full_name=view_full_name,
                            view_name=target_db,
                            src_view=src_view)
                    views_hql += "msck repair table `{0}`;\n\n".format(
                        view_full_name)

            elif self.ibis_env == 'PERF' and len(only_domain) > 0:
                views_hql = (
                    'DROP VIEW IF EXISTS {view_full_name};\n'
                    'DROP TABLE IF EXISTS {view_full_name};\n'
                    'CREATE VIEW {view_full_name} AS '
                    'SELECT * FROM {src_view}; \n')
                views_hql = views_hql.format(
                    view_full_name=view_full_name,
                    src_view=src_view)
            else:

                views_hql = (
                    'DROP VIEW IF EXISTS `{view_full_name}`;\n'
                    'DROP TABLE IF EXISTS `{view_full_name}`;\n\n'
                    'CREATE DATABASE IF NOT EXISTS `{view_name}`;\n'
                    'CREATE EXTERNAL TABLE `{view_full_name}` ('
                    '{create_column_hql},\n `ingest_timestamp` string)\n'
                    'partitioned by (incr_ingest_timestamp string)\n'
                    "stored as parquet location 'hdfs://"
                    "/user/data/{target_dir}/live/';\n\n")

                views_hql = views_hql.format(view_full_name=view_full_name,
                                             view_name=target_db,
                                             create_column_hql=create_hql,
                                             target_dir=self.target_dir)

                views_hql += add_partition_hql
                views_hql += "msck repair table `{0}`;\n\n".format(
                    view_full_name)
            return views_hql

        def create_views_info(target_db):
            """used for invalidate and for zookeeper locks"""
            view_full_name = '{view_name}.{database}_{table_name}'
            view_full_name = view_full_name.format(
                view_name=target_db, database=self.clean_database,
                table_name=self.clean_table_name)
            views_lists = '{view_name} {database}_{table_name}\n'
            views_lists = views_lists.format(
                view_name=target_db, database=self.clean_database,
                table_name=self.clean_table_name)
            tables_to_impala_invalidate = "invalidate metadata {0};\n".format(
                view_full_name)
            return tables_to_impala_invalidate, views_lists

        if self.view:
            create_view_hql = (
                'SET mapred.job.queue.name={queue_name};\n'
                'SET mapreduce.map.memory.mb=8000;\n'
                'SET mapreduce.reduce.memory.mb=16000;\n'
            )
            if self.ibis_env == 'PERF':
                create_view_hql += 'SET hive.exec.dynamic.partition.mode='\
                    'nonstrict;\n\n'
            create_view_hql = create_view_hql.format(
                queue_name=self.queue_name)
            view_list = self.view.split('|')

            domain = self.domain_list
            domain = domain.split(",")
            domain = set(domain).intersection(view_list)
            domain = list(domain)

            if self.ibis_env == 'PERF':
                for view_name in view_list:
                    if len(view_list) != len(domain) and\
                            len(domain) > 0 and domain[0]:
                        if domain[0] == view_name.lower():
                            continue
                        table_hql = gen_view_hql(view_name, domain[0])
                    else:
                        table_hql = gen_view_hql(view_name)
                    impala_invalidate_stmts, views = create_views_info(
                        view_name)
                    create_view_hql += table_hql
                    impala_invalidate += impala_invalidate_stmts
                    views_info += views

            else:
                for view_name in view_list:
                    table_hql = gen_view_hql(view_name)
                    impala_invalidate_stmts, views = create_views_info(
                        view_name)
                    create_view_hql += table_hql
                    impala_invalidate += impala_invalidate_stmts
                    views_info += views
        else:
            create_view_hql = (
                'SET mapreduce.map.memory.mb=8000;\n'
                'SET mapreduce.reduce.memory.mb=16000;\n\n')

        return create_view_hql, impala_invalidate, views_info


def main():
    """Create avro_parquet.hql file"""
    target_dir = sys.argv[1]  # HDFS
    sqoop_jars = sys.argv[2]  # Jar files
    hive2_jdbc_url = sys.argv[3]  # Hive jdbc_url
    ingest_time = sys.argv[4]  # ingest_time

    # Either "full_load" or "incremental"
    # - currently only processing "incremental"
    rpc_method = sys.argv[5]
    queue_name = sys.argv[6]

    conn_mgr = ConnectionManager(
        os.environ['source_database_name'],
        os.environ['source_table_name'],
        os.environ['source_schema_name'],
        os.environ['domain'],
        os.environ['jdbcurl'],
        os.environ['connection_factories'],
        os.environ['db_username'],
        os.environ['password_file'],
        os.environ['views'],
        os.environ['PERF_ENV'],
        os.environ['DOMAIN_LIST'],
        os.environ['IMPALA_HOST'],
        ingest_time,
        target_dir,
        sqoop_jars,
        hive2_jdbc_url,
        queue_name)

    if rpc_method == 'incremental':
        # Standard HQL - for creating / inserting into Parquet stage
        generic_hql = conn_mgr.get_hql(rpc_method)
        merge_table_hql = conn_mgr.get_incremental_hql()

        with open("incr_create_table.hql", 'wb') as fh_incr:
            fh_incr.write(generic_hql)
        with open("incr_merge_partition.hql", 'wb') as fh_incr:
            fh_incr.write(merge_table_hql)

    elif rpc_method == 'full_load':
        create_ingest_table_hql = conn_mgr.create_ingest_table()
        create_parquet_live_hql = conn_mgr.create_parquet_live()
        full_ingest_hql = conn_mgr.get_hql()

        with open("avro_parquet.hql", "a") as fileh:
            fileh.write(create_ingest_table_hql)
            fileh.write(full_ingest_hql)
            print 'SUCCESS: created avro_parquet.hql file'
        with open("parquet_live.hql", "a") as fileh:
            fileh.write(create_parquet_live_hql)
            print 'SUCCESS: created parquet_live.hql file'
    else:
        print 'Error: Unknown rpc_method:{0}'.format(rpc_method)

    view_hql, impl_txt, views_info_txt = conn_mgr.create_externaltable(
        rpc_method)

    if view_hql:
        create_view_file = "views.hql"
        with open(create_view_file, 'wb') as fileh:
            fileh.write(view_hql)
            print 'SUCCESS: created {0} file'.format(create_view_file)

    if impl_txt:
        invalidate_views_file = "views_invalidate.txt"
        with open(invalidate_views_file, 'wb') as fileh:
            fileh.write(impl_txt)
            print 'SUCCESS: created {0} file'.format(invalidate_views_file)

    if views_info_txt:
        views_info_file = "views_info.txt"
        with open(views_info_file, 'wb') as fileh:
            fileh.write(views_info_txt)
            print 'SUCCESS: created {0} file'.format(views_info_file)


if __name__ == "__main__":
    if len(sys.argv) < 6:
        print "command line args are not proper"
        sys.exit(1)
    main()
