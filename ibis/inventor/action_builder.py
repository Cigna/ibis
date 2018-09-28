"""Oozie action builder."""
import shutil
import os
from collections import OrderedDict
from ibis.custom_logging import get_logger
from ibis.utilities.sqoop_helper import SqoopHelper
from ibis.model.join import Join
from ibis.model.hive import Hive
from ibis.model.shell import Shell
from ibis.model.sqoop import Sqoop
from ibis.model.impala import Impala
from ibis.utilities.utilities import Utilities
from ibis.inventor.dsl_parser import DSLParser


class ActionBuilder(object):
    """Utilizes building blocks to generated workflow"""

    def __init__(self, cfg_mgr):
        self.cfg_mgr = cfg_mgr
        self.sqooper = SqoopHelper(self.cfg_mgr)
        self.logger = get_logger(self.cfg_mgr)
        self.it_table = None
        self.workflowName = None
        self.default_actions = (
            'import_prep', 'import', 'avro', 'avro_parquet',
            'quality_assurance', 'qa_data_sampling', 'parquet_swap',
            'parquet_live', 'views', 'refresh')
        self.custom_action_scripts = []
        self.error_to_action = 'kill'
        self.auto_query = False
        self.action_names = OrderedDict()
        self._set_action_names()
        self.dsl_parser = DSLParser(self.cfg_mgr, self.default_actions,
                                    self.cfg_mgr.requests_dir)
        self.rules = self.dsl_parser.generate_rules(self.action_names)
        self.request_oracle_view = False

    def _set_action_names(self):
        """setup action names"""
        self.action_names['import_prep'] = 'import_prep'
        self.action_names['import'] = 'import'
        self.action_names['avro'] = 'avro'
        self.action_names['avro_parquet'] = 'avro_parquet'
        self.action_names['quality_assurance'] = 'quality_assurance'
        self.action_names['qa_data_sampling'] = 'qa_data_sampling'
        self.action_names['parquet_swap'] = 'parquet_swap'
        self.action_names['parquet_live'] = 'parquet_live'
        self.action_names['views'] = 'views'
        self.action_names['refresh'] = 'refresh'
        for action in self.default_actions:
            if action not in self.action_names.keys():
                raise ValueError('Unrecognized action name: ' + action)

    def get_hdfs_files_path(self):
        """Returns the path of hdfs path of .py and .sh files
        used for running workflow"""
        return "/user/dev/oozie/workspaces/ibis/lib/ingest{0}/".format(
            self.cfg_mgr.hdfs_ingest_version)

    def gen_property_xml(self, prop_name, prop_value):
        """Returns the property xml block"""
        _xml = ('<property>\n<name>{property_name}'
                '</name>\n<value>{property_value}</value>\n</property>\n')
        _xml = _xml.format(property_name=prop_name, property_value=prop_value)
        return _xml

    def gen_fork_xml(self, name, paths, fork_type, is_sub=False):
        """Returns fork xml. Fork type option staggered or concurrent"""
        # Assumption: Fork goes to multiple import_prep or to
        #  an avro and import_prep
        fork = ''
        if fork_type == 'concurrent':
            fork = '\n\t<fork name=\"{name}\">\n'.format(name=name)
            for path in paths:
                if not is_sub:
                    _tmp = '\t\t<path start=\"{path}_{action_name}\"/>\n'
                    _tmp = _tmp.format(
                        path=path,
                        action_name=self.action_names['import_prep'])
                    fork += _tmp
                else:
                    fork += '\t\t<path start=\"{path}\"/>\n'.format(path=path)
            fork += '\t</fork>\n'
        elif fork_type == 'staggered':
            if len(paths) > 2:
                err_msg = 'Generation of staggered fork given ' \
                          'more than two tables {paths}'
                err_msg = err_msg.format(paths=paths)
                self.logger.error(err_msg)
            if not is_sub:
                paths[0] += '_' + self.action_names['avro']
                paths[1] += '_' + self.action_names['import_prep']
            fork = '\t<fork name=\"{name}\">\n'.format(name=name)
            fork += '\t\t<path start=\"{path}\"/>\n'.format(path=paths[0])
            fork += '\t\t<path start=\"{path}\"/>\n'.format(path=paths[1])
            fork += '\t</fork>\n'
        else:
            err_msg = 'Unknown fork type {fork_type} given'
            err_msg = err_msg.format(fork_type)
            self.logger.error(err_msg)
        return fork

    def gen_join_xml(self, path_from, path_to):
        """Return join xml"""
        join_params = {'cfg_mgr': self.cfg_mgr, 'action_type': 'join',
                       'name': path_from,
                       'to_node': path_to}
        return Join(**join_params).generate_action()

    def gen_import_prep_action(self, action_name):
        """Return import prep action xml"""
        params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'shell',
            'name': action_name, 'ok': 'unknown',
            'error': self.error_to_action, 'execute': 'import_prep.sh',
            'env_var': [
                'source_database_name={0}'.format(self.it_table.database),
                'source_table_name={0}'.format(self.it_table.table_name),
                'db_env={0}'.format(self.it_table.db_env),
                'target_dir={0}'.format(self.it_table.target_dir),
                'it_table={0}'.format(self.cfg_mgr.it_table),
                'it_table_host={0}'.format(self.cfg_mgr.workflow_host),
                'HADOOP_CONF_DIR=/etc/hadoop/conf',
                'hdfs_ingest_path={0}'.format(self.get_hdfs_files_path())],
            'file': [
                self.get_hdfs_files_path() + 'import_prep.sh#import_prep.sh']}
        return Shell(**params)

    def get_add_import_args(self, fetch_size):
        """Returns a list of additional args for sqoop import
        method based on server type"""
        if self.it_table.is_oracle:
            is_view = self.check_view_table()
            if is_view:
                oracle = ['--fetch-size', fetch_size]
                self.request_oracle_view = True
            else:
                oracle = ['--fetch-size', fetch_size, '--direct']
            return oracle
        if self.it_table.is_teradata:
            teradata = ['--', '--batch-size', fetch_size,
                        '--relaxed-isolation',
                        '--input-method', 'split.by.amp', '--access-lock']
            return teradata
        else:
            # DB2, SQL Server and MySQL have the same arguments
            generic = ['--fetch-size', fetch_size, '--relaxed-isolation']
            return generic

    def add_postgre_schema(self, schema):
        """Postgres schema - sqoop"""
        if self.it_table.is_postgresql:
            postgresql = ['--', '--schema', schema]
            return postgresql
        else:
            postgresql = []
            return postgresql

    def check_view_table(self):
        """
        Return True if oracle database.table is view else false
        """
        results = self.sqooper.get_object_types(self.it_table.database,
                                                self.it_table.table_name,
                                                self.it_table.jdbcurl,
                                                self.it_table.username,
                                                self.it_table.password_file)
        if results[0][0].lower() == "view":
            return True
        else:
            return False

    def get_col_types(self, it_table):
        """Fetch column types through sqoop eval
        Args:
            it_table: instance of ibis.model.table.ItTable
        Returns:
            List of columns types. [[name, data_type]]
        """
        database = it_table.database
        schema = None

        if it_table.is_sqlserver:
            schema = it_table.schema
        elif it_table.is_postgresql:
            schema = it_table.schema
        else:
            schema = None

        return self.sqooper.get_column_types(
            database, it_table.table_name, it_table.jdbcurl,
            it_table.username, it_table.password_file, schema)

    def get_ts_columns(self, col_types, jdbc_url):
        """Fetch timestamp columns"""
        return self.sqooper.get_timestamp_columns(col_types, jdbc_url)

    def get_lob_columns(self, col_types, jdbc_url):
        """Fetch large object columns"""
        return self.sqooper.get_lob_columns(col_types, jdbc_url)

    def add_map_column_java(self, it_table, col_types):
        """Map timestamp and lob columns to java types"""
        args = []
        ts_columns = self.get_ts_columns(col_types, it_table.jdbcurl)
        blob_columns = self.get_lob_columns(col_types, it_table.jdbcurl)
        ts_lob_columns = ts_columns + blob_columns

        if ts_lob_columns:
            args.append('--map-column-java')
            col_arg = []
            for column_name in ts_lob_columns:
                if not (it_table.is_sqlserver or it_table.is_mysql or
                        it_table.is_postgresql or it_table.is_teradata):
                    column_name = column_name.upper()
                col_arg.append('{column}=String'.format(column=column_name))
            map_column_java = ','.join(col_arg)
            args.append(map_column_java)

        return args

    def get_sqoop_query(self, it_table, col_types):
        """Handle sqoop query"""
        new_col_names = []
        auto_query = False
        args = []

        if it_table.split_by and (it_table.split_by in it_table.query):
            err_msg = 'Error: Cannot use split_by column in custom sql_query!'
            raise ValueError(Utilities.print_box_msg(err_msg, 'x'))

        if it_table.check_column and \
                (it_table.check_column in it_table.query):
            err_msg = 'Error: Cannot use check_column in custom sql_query!'
            raise ValueError(Utilities.print_box_msg(err_msg, 'x'))

        if it_table.query:
            # user provided sql_query in request file
            custom_query = ' AND ${sql_query} '
        else:
            custom_query = ''

        for info in col_types:
            col_name = info[0]
            col_type = info[1]
            if self.sqooper.column_has_special_chars(col_name) or \
                    (it_table.is_sqlserver and it_table.schema != 'dbo') or \
                    (it_table.is_oracle and '$' in it_table.table_name):
                auto_query = True
                new_col_name = self.sqooper.convert_special_chars(col_name)

                if it_table.is_sqlserver:
                    new_col_name_select = '[{0}] AS [{1}]'
                elif it_table.is_oracle:
                    new_col_name_select = '"{0}" AS {1}'
                    if col_type.lower() == 'xmltype':
                        new_col_name_select = 't.{0}.getclobval() {1}'
                else:
                    new_col_name_select = '{0} AS {1}'

                new_col_name_select = new_col_name_select.format(
                    col_name, new_col_name)
                new_col_names.append(new_col_name_select)
            elif it_table.is_oracle and col_type.lower() == 'xmltype':
                auto_query = True
                col_name = 't.{0}.getclobval() {0}'.format(col_name)
                new_col_names.append(col_name)
            elif it_table.is_postgresql and \
                    (col_type.lower() in ['xid', 'oid', 'array', 'macaddr']):
                auto_query = True
                col_name = '{0} as {0}'.format(col_name)
                new_col_names.append(col_name)
            else:
                new_col_names.append(col_name)

        if auto_query or it_table.query:
            from_table = it_table.full_sql_name

            if it_table.is_oracle:
                from_table = '{db}.{table} t'
                from_table = from_table.format(db=it_table.database,
                                               table=it_table.table_name)

            if it_table.is_sqlserver:
                from_table = '[{db}].[{schema}].[{table}]'
                from_table = from_table.format(
                    db=it_table.database, schema=it_table.schema,
                    table=it_table.table_name)

            if not it_table.is_oracle or auto_query:
                sqoop_query = ('SELECT {0} FROM {1} WHERE 1=1 {2}'
                               ' AND $CONDITIONS')
                columns_stmt = ', '.join(new_col_names)
                sqoop_query = sqoop_query.format(
                    columns_stmt, from_table, custom_query)
                args.append('--query')
                args.append(sqoop_query)
            else:
                args.append('--where')
                args.append(it_table.query)
        self.auto_query = auto_query
        return args

    def add_sqoop_validate(self):
        """
        Adding SQOOP validate for full ingests.
        :return:
        """
        # TODO: Make these be a property file for the env
        validator = 'org.apache.sqoop.validation.RowCountValidator'
        validation_threshold = 'org.apache.sqoop.validation.AbsoluteValidationThreshold'
        validation_failurehandler = 'org.apache.sqoop.validation.' \
                                    'AbortOnFailureHandler'
        args = ['--validate', '--validator', validator,
                '--validation-threshold', validation_threshold,
                '--validation-failurehandler', validation_failurehandler]
        return args

    def handle_incremental(self, args, incremental, sqoop_where_query):
        """Handles incremental args"""
        if incremental:
            # meaning there's a timestamp(check_column) column
            if incremental['incremental'] == "lastmodified" \
                    and sqoop_where_query:
                args += ['--where', sqoop_where_query]
            elif incremental['incremental'] == "append":
                # incremental sqoop does not allow deletion of target_dir
                if '--delete-target-dir' in args:
                    args.remove('--delete-target-dir')

                args += ['--check-column', incremental['check_column'],
                         '--incremental', incremental['incremental'],
                         '--last-value', incremental['last_value']]
            else:
                err_msg = "Unknown incremental options: incremental: {0}, " \
                          "sqoop_where_query: {1}"
                err_msg = err_msg.format(incremental, sqoop_where_query)
                raise ValueError(Utilities.print_box_msg(err_msg, 'x'))

    def _add_split_by(self, sqoop_args, col_types):
        """Adds split by to sqoop import"""
        valid_column = False
        for col_info in col_types:
            col_name = col_info[0]
            if col_name == self.it_table.split_by:
                valid_column = True
                break
        if not valid_column:
            columns = self.sqooper.get_column_names(col_types)
            column_names = "', '".join(columns)
            err_msg = ("Not a valid column name for split_by: '{0}'\n\n"
                       "Possible valid column names: '{1}'")
            err_msg = err_msg.format(self.it_table.split_by, column_names)
            raise ValueError(Utilities.print_box_msg(err_msg, 'x'))

        sqoop_args.append('--split-by')
        sqoop_args.append(self.it_table.split_by)

    def gen_import_action(self, action_name, fields_terminated_by=None,
                          incremental=None, sqoop_where_query=None):
        """Returns the import action xml
        Args:
        """
        args = []
        config = [{'fs.hdfs.impl.disable.cache': 'true'}]
        jceks, password = self.it_table.split_password_file
        target_dir = '/user/data/ingest/{0}'.format(self.it_table.target_dir)

        args += ['--verbose', '--connect', self.it_table.jdbcurl,
                 '--target-dir', target_dir, '--delete-target-dir',
                 '--username', self.it_table.username]
        # Default args
        if jceks:
            d_jceks = '-D hadoop.security.credential.provider.path={jceks}'
            d_jceks = d_jceks.format(jceks=jceks)
            # * All -D settings must occur first
            args.insert(0, d_jceks)
            args += ['--password-alias', password]
        else:
            args += ['--password-file', password]

        # Either ingest as AVRO or use tab delimited files
        if fields_terminated_by:
            args += ['--fields-terminated-by', fields_terminated_by]
        else:
            args.insert(1, '--as-avrodatafile')

        col_types = self.get_col_types(self.it_table)

        if incremental:
            query_args = []
        else:
            query_args = self.get_sqoop_query(self.it_table, col_types)

        if query_args:
            if self.it_table.is_oracle:
                args += ['-m', 1]
            else:
                args += ['-m', self.it_table.mappers]
        else:
            args += ['-m', self.it_table.mappers]

        self.handle_incremental(args, incremental, sqoop_where_query)

        if not incremental and not query_args:
            # Incremental does not support automatic
            # Sqoop validation, but full does
            args += self.add_sqoop_validate()

        if self.it_table.is_teradata:
            args += ['--driver', self.it_table.connection_factories]

        if self.it_table.is_sqlserver and incremental and \
                self.it_table.schema != 'dbo':
            # set driver for non dbo incremental
            args += [
                '--driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver']

        if self.it_table.is_sqlserver:
            windows_auth = 'domain'
            if self.it_table.is_sqlserver and \
                    windows_auth in self.it_table.jdbcurl:
                # set driver for sqlserver windows authentication
                args += ['--driver', self.it_table.connection_factories]
            else:
                temp_txt = '-D sqoop.connection.factories={0}'
                temp_txt = temp_txt.format(self.it_table.connection_factories)
                args.insert(0, temp_txt)

        # Add connection factories and split by if provided and not oracle
        if not self.it_table.is_oracle:
            if self.it_table.connection_factories and \
                    not self.it_table.is_teradata and \
                    not self.it_table.is_sqlserver:
                temp_txt = '-D sqoop.connection.factories={0}'
                temp_txt = temp_txt.format(self.it_table.connection_factories)
                args.insert(0, temp_txt)
            if self.it_table.split_by:
                self._add_split_by(args, col_types)
        else:
            args.insert(0, '-D oraoop.timestamp.string=false')
            if query_args and self.it_table.split_by:
                self._add_split_by(args, col_types)

        args += query_args

        if self.request_oracle_view:
            mapper_index = args.index('-m')
            args[mapper_index + 1] = 1

        # Check for columns being casted to timestamp
        args += self.add_map_column_java(self.it_table, col_types)

        args += self.add_postgre_schema(self.it_table.schema)

        if self.it_table.is_sqlserver and incremental and \
                self.it_table.schema != 'dbo':
            # for non dbo schema - SQL SERVER - Incremental
            table = self.it_table.full_sql_name
        elif self.it_table.is_postgresql and incremental and \
                self.it_table.schema != 'dbo':
            # for non dbo schema - postgre SERVER - Incremental
            table = self.it_table.table_name
        elif self.it_table.is_sqlserver or self.it_table.is_postgresql or \
                self.it_table.is_mysql or self.it_table.is_teradata:
            table = self.it_table.table_name
        else:
            table = self.it_table.full_sql_name

        if self.it_table.is_sqlserver or self.it_table.is_mysql or \
                self.it_table.is_postgresql:
            # case sensitive
            table_arg = table
        else:
            table_arg = table.upper()

        if (not query_args) or (self.it_table.is_oracle and
                                not self.auto_query):
            _index = args.index('--username')
            # insert before username
            args.insert(_index, table_arg)
            args.insert(_index, '--table')

        args = args + self.get_add_import_args(self.it_table.fetch_size)

        config_properties = self.get_sqoop_credentials_config(
            config, self.it_table.password_file)
        sqoop_param = {'cfg_mgr': self.cfg_mgr, 'action_type': 'sqoop',
                       'name': action_name,
                       'ok': 'unknown', 'error': self.error_to_action,
                       'config': config_properties,
                       'command': 'import', 'arg': args}

        return Sqoop(**sqoop_param)

    def gen_avro_action(self, action_name):
        """Return avro action xml"""
        params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'shell',
            'name': action_name, 'ok': 'unknown',
            'error': self.error_to_action, 'execute': 'avro_parquet.sh',
            'env_var': ['target_dir={0}'.format(self.it_table.target_dir),
                        'hive2_jdbc_url=${hive2_jdbc_url}',
                        'HADOOP_CONF_DIR=/etc/hadoop/conf',
                        'hdfs_ingest_path={0}'.format(
                            self.get_hdfs_files_path())],
            'file': [self.get_hdfs_files_path() +
                     'avro_parquet.sh#avro_parquet.sh']}
        return Shell(**params)

    def gen_parquet_clone_action(self, action_name, source_table_name,
                                 source_database_name, source_dir, ok, error):
        """Return parquet export xml"""
        params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'shell',
            'name': action_name,
            'ok': ok, 'error': error, 'execute': 'parquet_export.sh',
            'env_var': ['table_name={0}'.format(source_table_name),
                        'target_dir={0}'.format(source_dir),
                        'database={0}'.format(source_database_name),
                        'hive2_jdbc_url=${hive2_jdbc_url}',
                        'HADOOP_CONF_DIR=/etc/hadoop/conf',
                        'hdfs_ingest_path={0}'.format(
                            self.get_hdfs_files_path())],
            'file': [
                self.get_hdfs_files_path() + 'parquet_export.sh#'
                                             'parquet_export.sh']}
        parquet_export = Shell(**params)
        return parquet_export.generate_action()

    def gen_exec_hive_clone_action(self, action_name, source_table, ok, error):
        """Returns hive action that executes the hql generated
        by parquet_export.sh"""
        temp_folder = 'tmp'
        params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'hive',
            'name': action_name, 'ok': ok, 'error': error,
            'script': '${nameNode}/' + temp_folder + '/' + source_table +
                      '_clone_hql/parquet_export.hql'}
        hive = Hive(**params)
        return hive.generate_action()

    def gen_sqoop_oracle_export_action(
            self, tbl, jdbc_url, source_dir, source_table_name, update_key,
            target_database_name, target_table_name, user_name, pass_alias):
        config = [{'fs.hdfs.impl.disable.cache': 'true'}]
        config_properties = self.get_sqoop_credentials_config(config,
                                                              pass_alias)
        sqoop_params = {'cfg_mgr': self.cfg_mgr, 'action_type': 'sqoop',
                        'name': tbl + '_sqoop_export', 'ok': 'oozie_cb_ok',
                        'error': 'kill',
                        'config': config_properties,
                        'command': 'export'}
        temp_folder = 'tmp'
        export_args = []

        if 'jceks' in pass_alias:
            jceks, pwd_alias = pass_alias.split('#')
            d_args = '-D hadoop.security.credential.provider.path={jceks}'
            d_args = d_args.format(jceks=jceks)
            export_args.insert(0, d_args)
            password_arg = '--password-alias'
            password = pwd_alias
        else:
            password_arg = '--password-file'
            password = pass_alias

        debug_data = '-D org.apache.sqoop.export.text.dump_data_on_error=true'
        export_args.insert(0, debug_data)

        if len(update_key) > 0:
            update_mode = 'allowinsert'
        else:
            # updateonly
            update_mode = 'updateonly'

        export_args += [
            '--verbose', '--connect', jdbc_url, '--username',
            user_name, password_arg, password, '--table',
            target_database_name.upper() + '.' + target_table_name.upper(),
            '--export-dir',
            '/' + temp_folder + '/' + source_table_name + '_clone',
            '--update-key', update_key, '--update-mode', update_mode,
            '-m', '10',
            '--input-null-string', '\\\\N',
            '--input-null-non-string', '\\\\N',
            '--input-fields-terminated-by', '|']
        sqoop_params['arg'] = export_args

        sqoop = Sqoop(**sqoop_params)
        return sqoop.generate_action()

    def gen_sqoop_teradata_export_action(
            self, tbl, jdbc_url, source_dir, source_table_name,
            target_database_name, target_table_name, user_name, pass_alias):
        config = [{'fs.hdfs.impl.disable.cache': 'true'}]
        config_properties = self.get_sqoop_credentials_config(
            config, pass_alias)
        sqoop_params = {'cfg_mgr': self.cfg_mgr, 'action_type': 'sqoop',
                        'name': tbl + '_sqoop_export', 'ok': 'oozie_cb_ok',
                        'error': 'kill', 'config': config_properties,
                        'command': 'export'}

        temp_folder = 'tmp'
        export_args = []

        if 'jceks' in pass_alias:
            jceks, pwd_alias = pass_alias.split('#')
            d_args = '-D hadoop.security.credential.provider.path={jceks}'
            d_args = d_args.format(jceks=jceks)
            export_args.insert(0, d_args)
            password_arg = '--password-alias'
            password = pwd_alias
        else:
            password_arg = '--password-file'
            password = pass_alias

        debug_data = '-D org.apache.sqoop.export.text.dump_data_on_error=true'
        export_args.insert(0, debug_data)

        export_args += [
            '--verbose', '--connect', jdbc_url,
            '--username', user_name, password_arg, password,
            '--table',
            target_database_name.upper() + '.' + target_table_name.upper(),
            '--export-dir',
            '/' + temp_folder + '/' + source_table_name + '_clone',
            '-m', '10',
            '--input-null-string', '\\\\N',
            '--input-null-non-string', '\\\\N',
            '--input-fields-terminated-by', '|']

        sqoop_params['arg'] = export_args
        sqoop = Sqoop(**sqoop_params)
        return sqoop.generate_action()

    def gen_parquet_clone_action_RDBMS(self, action_name, source_table_name,
                                       source_database_name, source_dir,
                                       ok, error):
        """Return parquet export xml"""
        params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'shell',
            'name': action_name,
            'ok': ok, 'error': error, 'execute': 'export_prep.sh',
            'env_var': ['table_name={0}'.format(source_table_name),
                        'source_dir={0}'.format(source_dir),
                        'database={0}'.format(source_database_name),
                        'hive2_jdbc_url=${hive2_jdbc_url}',
                        'HADOOP_CONF_DIR=/etc/hadoop/conf',
                        'hdfs_export_path={0}'.format(
                            self.get_hdfs_files_path())],
            'file': [
                self.get_hdfs_files_path() + 'export_prep.sh#'
                                             'export_prep.sh'],
            'capture_output': 'true'}
        export_prep = Shell(**params)
        return export_prep.generate_action()

    def gen_exec_hive_clone_action_RDBMS(self, action_name, source_table,
                                         source_database, ok, error):
        """Returns hive action that executes the hql generated
        by export_prep.sh
        """
        params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'hive',
            'name': action_name, 'ok': ok, 'error': error,
            'script': '${nameNode}' + self.cfg_mgr.export_hdfs_root +
                      source_database + '/' + source_table + '_clone_hql/'
                      'parquet_export.hql'}
        hive = Hive(**params)
        return hive.generate_action()

    def gen_exec_check_action(self, action_name, tbl, driver, source_database,
                              jdbc_url, user_name, pass_alias,
                              target_database_name, jdbc_database,
                              target_table_name, ok, error):
        if 'jceks' in pass_alias:
            jceks, pwd_alias = pass_alias.split('#')
            db_password = pwd_alias
        else:
            db_password = pass_alias
            jceks = 'None'

        if 'sqlserver' in jdbc_url:
            database = jdbc_database
        elif 'postgresql' in jdbc_url:
            database = jdbc_database
        else:
            database = target_database_name

        params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'shell',
            'name': action_name,
            'ok': ok, 'error': error, 'execute': 'check_export.sh',
            'env_var': ['target_schema={0}'.format(database),
                        'connection_factories={0}'.format(driver),
                        'jdbc_url={0}'.format(jdbc_url),
                        'database={0}'.format(target_database_name),
                        'target_table={0}'.format(target_table_name),
                        'user_name={0}'.format(user_name),
                        'password_alias={0}'.format(db_password),
                        'jceks={0}'.format(jceks),
                        'source_database_name={0}'.format(source_database),
                        'source_table_name={0}'.format(tbl),
                        'HADOOP_CONF_DIR=/etc/hadoop/conf',
                        'hdfs_export_path={0}'.format(
                            self.get_hdfs_files_path())],
            'file': [
                self.get_hdfs_files_path() + 'check_export.sh#'
                                             'check_export.sh']}
        check_export = Shell(**params)
        return check_export.generate_action()

    def gen_clone_cleanup_action(self, action_name, source_table,
                                 source_database, ok, error):
        """Returns hive action that executes the hql generated
        by clone_delete.sh"""
        params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'hive',
            'name': action_name, 'ok': ok, 'error': error,
            'script': '${nameNode}' + self.cfg_mgr.export_hdfs_root +
                      source_database + '/' + source_table +
                      '_clone_hql/clone_delete.hql'}
        clone_cleanup = Hive(**params)
        return clone_cleanup.generate_action()

    def gen_quality_assurance_export_action(
            self, action_name, source_table_name, source_database,
            source_db_name, jdbc_database, jdbc_url, connection_factories,
            user_name, jceks, password, domain, ok, error):
        """Return the quality assurance action xml."""
        if not jdbc_database:
            jdbc_database = 'None'
        if 'sqlserver' in jdbc_url:
            original_source_db_name = source_db_name
            source_db_name = jdbc_database
            jdbc_database = original_source_db_name
        if 'db2' in jdbc_url:
            original_source_db_name = source_db_name
            source_db_name = jdbc_database
            jdbc_database = original_source_db_name
        if 'oracle' in jdbc_url:
            original_source_db_name = source_db_name
            source_db_name = jdbc_database
            jdbc_database = original_source_db_name
        if 'mysql' in jdbc_url:
            original_source_db_name = source_db_name
            source_db_name = jdbc_database
            jdbc_database = original_source_db_name

        workflow = self.workflowName
        params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'shell',
            'name': action_name, 'ok': ok, 'error': error,
            'execute': 'quality_assurance_export.sh',
            'env_var': [
                'jdbc_url={0}'.format(jdbc_url),
                'connection_factories={0}'.format(
                    connection_factories),
                'user_name={0}'.format(user_name),
                'password_alias={0}'.format(password),
                'jceks={0}'.format(jceks),
                'source_table_name={0}'.format(source_table_name),
                'source_database_name={0}'.format(source_database),
                'target_schema={0}'.format(source_db_name),
                'target_table_name={0}'.format(domain),
                'target_database={0}'.format(jdbc_database),
                'workflowName={0}'.format(workflow),
                'hdfs_export_path={0}'.format(self.get_hdfs_files_path())],
            'file': [self.get_hdfs_files_path() +
                     'quality_assurance_export.sh#'
                     'quality_assurance_export.sh']}
        qa_shell = Shell(**params)
        return qa_shell.generate_action()

    def gen_sqoop_database_export_action(
            self, source_table, jdbc_url, source_dir, source_database,
            update_key, target_database, target_table, user_name, pass_alias,
            mappers, staging_database):
        """Sqoop export xml"""
        if 'teradata' in jdbc_url:
            factories = ('com.cloudera.connector.teradata.'
                         'TeradataManagerFactory')
            config = [
                {'fs.hdfs.impl.disable.cache': 'true'},
                {'sqoop.connection.factories': factories}]
            arg_table = target_table.upper()
        elif 'sqlserver' in jdbc_url:
            config = [{'fs.hdfs.impl.disable.cache': 'true'}]
            arg_table = target_table
        elif 'mysql' in jdbc_url:
            config = [{'fs.hdfs.impl.disable.cache': 'true'}]
            arg_table = target_table
        else:
            config = [{'fs.hdfs.impl.disable.cache': 'true'}]
            arg_table = target_database.upper() + '.' + target_table.upper()

        export_args = []
        delim = '\134001'

        if staging_database:
            staging_db = staging_database
        else:
            staging_db = self.cfg_mgr.staging_database

        if 'jceks' in pass_alias:
            jceks, pwd_alias = pass_alias.split('#')
            d_args = '-D hadoop.security.credential.provider.path={jceks}'
            d_args = d_args.format(jceks=jceks)
            export_args.insert(0, d_args)
            password_arg = '--password-alias'
            password = pwd_alias
        else:
            password_arg = '--password-file'
            password = pass_alias

        debug_data = '-D org.apache.sqoop.export.text.dump_data_on_error=true'
        export_args.insert(0, debug_data)

        export_args += [
            '--verbose', '--connect', jdbc_url,
            '--username', user_name, password_arg, password,
            '--table', arg_table, '--export-dir',
            self.cfg_mgr.export_hdfs_root + source_database +
            '/' + source_table + '_clone',
            '-m', mappers,
            '--input-null-string', '\\\\N',
            '--input-null-non-string', '\\\\N',
            '--input-fields-terminated-by', delim]

        if update_key != 'null':
            export_args += ['--update-key', update_key,
                            '--update-mode', 'allowinsert']

        if 'teradata' in jdbc_url:
            export_args += ['--', '--staging-database', staging_db]
        else:
            column_names = \
                "${{wf:actionData('{0}_export_prep')['columnNames']}}"
            column_names = column_names.format(source_table)
            export_args += ['--columns', column_names]

        config_properties = self.get_sqoop_credentials_config(
            config, pass_alias)
        sqoop_params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'sqoop',
            'name': source_table + '_export', 'ok': source_table + '_cleanup',
            'error': 'oozie_cb_fail', 'config': config_properties,
            'command': 'export'
        }
        sqoop_params['arg'] = export_args
        sqoop = Sqoop(**sqoop_params)
        return sqoop.generate_action()

    def gen_avro_parquet_action(self, action_name):
        """Returns the avro parquet action xml"""
        params = {'cfg_mgr': self.cfg_mgr, 'action_type': 'hive',
                  'name': action_name, 'ok': 'unknown',
                  'error': self.error_to_action,
                  'script': '${nameNode}/user/data/' +
                            self.it_table.target_dir + '/gen/avro_parquet.hql'}
        return Hive(**params)

    def gen_quality_assurance_action(self, action_name, ingestion_type):
        """Return the quality assurance action xml."""
        params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'shell',
            'name': action_name, 'ok': 'unknown',
            'error': self.error_to_action, 'execute': 'quality_assurance.sh',
            'env_var': ['ingestion_type={0}'.format(ingestion_type),
                        'target_dir={0}'.format(self.it_table.target_dir),
                        'HADOOP_CONF_DIR=/etc/hadoop/conf',
                        'hdfs_ingest_path={0}'.format(
                            self.get_hdfs_files_path())],
            'file': [self.get_hdfs_files_path() + 'quality_assurance.sh#'
                                                  'quality_assurance.sh']}

        if ingestion_type == 'incremental':
            params['env_var'].append(
                'workflowName={0}'.format(self.workflowName))
        return Shell(**params)

    def gen_parquet_swap_action(self, action_name):
        """Returns the parquet_swap action xml"""
        params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'shell',
            'name': action_name, 'ok': 'unknown',
            'error': self.error_to_action, 'execute': 'parquet_swap.sh',
            'env_var': [
                'target_dir={0}'.format(self.it_table.target_dir),
                'hive2_jdbc_url=${hive2_jdbc_url}',
                'HADOOP_CONF_DIR=/etc/hadoop/conf',
                'hdfs_ingest_path={0}'.format(self.get_hdfs_files_path())],
            'file': [self.get_hdfs_files_path() +
                     'parquet_swap.sh#parquet_swap.sh']}
        return Shell(**params)

    def gen_parquet_live(self, action_name):
        """Returns the avro parquet action xml"""
        params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'hive',
            'name': action_name, 'ok': 'unknown',
            'error': self.error_to_action,
            'script': '${nameNode}/user/data/' + self.it_table.target_dir +
                      '/gen/parquet_live.hql'}
        return Hive(**params)

    def gen_create_views_action(self, action_name):
        """Returns the create views action xml"""
        script_name = '/views.hql'
        params = {'cfg_mgr': self.cfg_mgr, 'action_type': 'hive',
                  'name': action_name, 'ok': 'unknown',
                  'error': self.error_to_action,
                  'script': '${nameNode}/user/data/' +
                            self.it_table.target_dir + '/gen' + script_name}
        return Hive(**params)

    def gen_refresh(self, action_name):
        """Return import prep action xml"""
        params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'shell',
            'name': action_name, 'ok': 'unknown',
            'error': self.error_to_action, 'execute': 'impala_cleanup.sh',
            'env_var': ['target_dir={0}'.format(self.it_table.target_dir),
                        'HADOOP_CONF_DIR=/etc/hadoop/conf',
                        'hdfs_ingest_path={0}'.format(
                            self.get_hdfs_files_path())],
            'file': [
                self.get_hdfs_files_path() + 'impala_cleanup.sh#'
                                             'impala_cleanup.sh']}
        return Shell(**params)

    def gen_oracle_table_export(
            self, source_table_name, source_database_name, source_dir,
            jdbc_url, update_key, target_table_name, target_database_name,
            user_name, password):
        """Returns the xml for generating clone hql, executing hql and
        performing Oozie export"""
        # Default the error to the oozie_cb_fail action
        error = 'oozie_cb_fail'
        tbl = source_table_name
        database = source_database_name
        # BUILD HQL FILE
        clone_parquet_xml = self.gen_parquet_clone_action(
            tbl + '_genhql', tbl, database, source_dir, tbl + '_runhql', error)
        # EXEC HQL
        export_table_xml = self.gen_exec_hive_clone_action(
            tbl + '_runhql', tbl, tbl + '_sqoop_export', error)

        sqoop_export_xml = self.gen_sqoop_oracle_export_action(
            tbl, jdbc_url, source_dir, source_table_name, update_key,
            target_database_name,
            target_table_name, user_name, password)
        return clone_parquet_xml + export_table_xml + sqoop_export_xml

    def gen_teradata_table_export(
            self, source_table_name, source_database_name, source_dir,
            jdbc_url, target_table_name, target_database_name, user_name,
            password):
        """Returns the xml for generating clone hql,
        executing hql and performing Oozie export
        """
        # Default the error to the oozie_cb_fail action
        error = 'oozie_cb_fail'
        tbl = source_table_name
        database = source_database_name
        # BUILD HQL FILE
        clone_parquet_xml = self.gen_parquet_clone_action(
            tbl + '_genhql', tbl, database, source_dir, tbl + '_runhql', error)
        # EXEC HQL
        export_table_xml = self.gen_exec_hive_clone_action(
            tbl + '_runhql', tbl, tbl + '_sqoop_export', error)

        sqoop_export_xml = self.gen_sqoop_teradata_export_action(
            tbl, jdbc_url, source_dir, source_table_name, target_database_name,
            target_table_name, user_name, password)
        return clone_parquet_xml + export_table_xml + sqoop_export_xml

    def gen_database_table_export(self, tables):
        """Returns the xml for generating clone hql, executing hql and
        performing Oozie export """
        # Default the error to the oozie_cb_fail action
        error = 'oozie_cb_fail'
        tbl = tables[0].table_name
        db = tables[0].database
        jdbc_url = tables[0].jdbcurl
        target_tbl = tables[0].target_table
        target_schema = tables[0].target_schema
        source_dir = tables[0].source_dir
        source_database = tables[0].database
        user_name = tables[0].username
        password = tables[0].password_file
        connection_factories = tables[0].connection_factories
        jdbc_database = tables[0].sqlserver_database
        mappers = tables[0].mappers
        driver = tables[0].connection_factories
        staging_database = tables[0].staging_database

        if tables[0].update_key:
            update_key = tables[0].update_key
        else:
            update_key = 'null'

        if 'jceks' in password:
            jceks_value, password_value = password.split('#')
        else:
            jceks_value = None
            password_value = password

        # BUILD HQL FILE
        clone_parquet_xml = self.gen_parquet_clone_action_RDBMS(
            tbl + '_export_prep', tbl, db, source_dir, tbl +
            '_parquet_text', error)

        if update_key != 'null':
            # EXEC HQL
            export_table_xml = self.gen_exec_hive_clone_action_RDBMS(
                tbl + '_parquet_text', tbl, db, tbl + '_export', error)

            export_prep_xml = clone_parquet_xml + export_table_xml
        else:
            export_table_xml = self.gen_exec_hive_clone_action_RDBMS(
                tbl + '_parquet_text', tbl, db, tbl +
                '_pre_quality_assurance', error)

            check_table_xml = self.gen_exec_check_action(
                tbl + '_pre_quality_assurance', tbl, driver, source_database,
                jdbc_url, user_name, password, target_schema, jdbc_database,
                target_tbl, tbl + '_export', error)

            export_prep_xml = clone_parquet_xml + export_table_xml + \
                check_table_xml

        sqoop_export_xml = self.gen_sqoop_database_export_action(
            tbl, jdbc_url, source_dir, db, update_key,
            target_schema, target_tbl, user_name, password, mappers,
            staging_database)

        # Clone (delimited text file) cleanup
        export_cleanup_xml = self.gen_clone_cleanup_action(
            tbl + '_cleanup', tbl, db, tbl + '_post_quality_assurance', error)

        # Quality Assurance Rules Action
        quality_assurance_xml = self.gen_quality_assurance_export_action(
            tbl + '_post_quality_assurance', tbl, source_database,
            target_schema, jdbc_database, jdbc_url,
            connection_factories, user_name, jceks_value, password_value,
            target_tbl, 'oozie_cb_ok', error)

        return export_prep_xml + sqoop_export_xml + export_cleanup_xml + \
            quality_assurance_xml

    def _get_action_name(self, suffix_name):
        """Returns action name appended with table name"""
        table_name = self.it_table.table_name
        table_name = Utilities.replace_special_chars(table_name)
        return table_name + '_' + suffix_name

    def gen_full_table_ingest(self, it_table, sqoop_to=None, final_ok_to=None):
        """Returns the xml of all of the actions required for an ingest.
        Typical ingestion flow:
            import_prep -> import -> avro -> avro_parquet ->
            quality_assurance -> qa_data_sampling -> parquet_swap ->
            parquet_live -> views -> refresh -> podium_profile
        Args:
            it_table: instance of ibis.model.table.ItTable
            sqoop_to: used for stagger sets sqoop action to value
            final_ok_to: used to set the final ok in parquet live action
        Returns:
            ingest_xml: concatnated xml of all actions
        """
        custom_rules = self.dsl_parser.get_custom_rules(it_table.actions)
        if custom_rules:
            self.logger.info('DSL config provided.')
            self.rules = custom_rules

        self.it_table = it_table
        self.error_to_action = 'oozie_cb_fail'
        final_ok_to = final_ok_to if final_ok_to else 'oozie_cb_ok'
        final_ok_to = Utilities.replace_special_chars(final_ok_to)
        sqoop_ok_to = it_table.table_name + '_avro'
        if sqoop_to:
            sqoop_ok_to = Utilities.replace_special_chars(sqoop_to)

        # IMPORT PREP ACTION
        import_prep_action = self.gen_import_prep_action(
            self._get_action_name(self.action_names['import_prep']))

        # SQOOP IMPORT ACTION
        import_action = self.gen_import_action(
            self._get_action_name(self.action_names['import']))

        # AVRO ACTION
        avro_action = self.gen_avro_action(
            self._get_action_name(self.action_names['avro']))

        # AVRO PARQUET ACTION
        avro_parquet_action = self.gen_avro_parquet_action(
            self._get_action_name(self.action_names['avro_parquet']))

        # Quality Assurance Action
        quality_assurance_action = self.gen_quality_assurance_action(
            self._get_action_name(self.action_names['quality_assurance']),
            "full_ingest")

        # Quality Assurance Data Sampling Action
        qa_data_sampling_action = self.gen_quality_assurance_action(
            self._get_action_name(self.action_names['qa_data_sampling']),
            "full_ingest_qa_sampling")

        # PARQUET SWAP ACTION
        parquet_swap_action = self.gen_parquet_swap_action(
            self._get_action_name(self.action_names['parquet_swap']))

        # PARQUET LIVE ACTION
        parquet_live_action = self.gen_parquet_live(
            self._get_action_name(self.action_names['parquet_live']))

        # View Action
        views_action = self.gen_create_views_action(
            self._get_action_name(self.action_names['views']))

        refresh_action = self.gen_refresh(
            self._get_action_name(self.action_names['refresh']))

        actions = []
        ingest_xml = ''

        for index, rule in enumerate(self.rules):
            if rule.action_id == self.action_names['import_prep']:
                actions.append(import_prep_action)
            elif rule.action_id == self.action_names['import']:
                actions.append(import_action)
            elif rule.action_id == self.action_names['avro']:
                actions.append(avro_action)
            elif rule.action_id == self.action_names['avro_parquet']:
                actions.append(avro_parquet_action)
            elif rule.action_id == self.action_names['quality_assurance']:
                actions.append(quality_assurance_action)
            elif rule.action_id == self.action_names['qa_data_sampling']:
                actions.append(qa_data_sampling_action)
            elif rule.action_id == self.action_names['parquet_swap']:
                actions.append(parquet_swap_action)
            elif rule.action_id == self.action_names['parquet_live']:
                actions.append(parquet_live_action)
            elif rule.action_id == self.action_names['views']:
                actions.append(views_action)
            elif rule.action_id == self.action_names['refresh']:
                actions.append(refresh_action)
            elif rule.action_id == 'hive_script':
                action_name = self._get_action_name('hive_' + str(index))
                script_name = self.gen_custom_workflow_scripts(
                    rule, self.dsl_parser.scripts_dir)
                hive_action = self.gen_hive_action(
                    action_name, self.cfg_mgr.custom_scripts_hql,
                    script_name, '', self.error_to_action)
                actions.append(hive_action)
            elif rule.action_id == 'shell_script':
                action_name = self._get_action_name('shell_' + str(index))
                script_name = self.gen_custom_workflow_scripts(
                    rule, self.dsl_parser.scripts_dir)
                shell_action = self.gen_shell_action(
                    action_name, self.cfg_mgr.custom_scripts_shell,
                    script_name, '', self.error_to_action)
                actions.append(shell_action)

        for current_action, next_action in Utilities.pairwise(actions):
            if current_action.get_action_type() == 'sqoop':
                current_action.ok = sqoop_ok_to
            else:
                current_action.ok = next_action.get_name()
            ingest_xml += current_action.generate_action()

        # generate last action
        last_action = actions[-1]
        last_action.ok = final_ok_to
        ingest_xml += last_action.generate_action()
        return ingest_xml

    def get_pdm_table_name(self, src_name, entity_name):
        """Gets entity name from ibis src name"""
        entity_name = entity_name.replace(src_name + '_', '', 1)
        return entity_name.upper()

    def gen_kite_ingest(self, source_table_name, source_database_name,
                        hdfs_loc, ok, error):
        """Return kite ingest action xml"""
        params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'shell',
            'name': source_table_name + "_kite_ingest",
            'ok': ok, 'error': error, 'execute': 'kite.sh',
            'env_var': [
                'hdfs_ingest_path={0}'.format(
                    self.get_hdfs_files_path())],
            'arg': ['{0}'.format(source_database_name),
                    '{0}'.format(source_table_name),
                    '{0}'.format(hdfs_loc)],
            'file': [
                self.get_hdfs_files_path() + 'kite.sh#'
                                             'kite.sh']}
        kite_ingest = Shell(**params)
        return kite_ingest.generate_action()

    def get_hive_credentials_config(self):
        """Returns hive config properties to add on hive.xml.mako template
        for hive_credentials action workflow.xml
        """
        config = [{'oozie.action.sharelib.for.hive': 'hive2',
                   'oozie.launcher.action.main.class':
                   'org.apache.oozie.action.hadoop.Hive2Main'}]
        return config

    def get_sqoop_credentials_config(self, config, password):
        """Returns sqoop config properties to add on sqoop.xml.mako template
        for sqoop_credentials action workflow.xml """
        if self.cfg_mgr.hadoop_credstore_password_disable is True and \
                (password is not None and ('jceks' in password)):
            config.append({
                'oozie.launcher.mapred.map.child.env':
                'HADOOP_CREDSTORE_PASSWORD=none'})
        return config

    def gen_custom_workflow_scripts(self, rule, scripts_dir):
        """Custom hql, shell scripts need to be commited to git.
        Args:
            rule: instance of ibis.inventor.dsl_parser.WorkflowRule
            scripts_dir: local unix directory where scripts are located
        """
        custom_script_path = rule.custom_action_script
        if custom_script_path[0] == '/':
            custom_script_path = custom_script_path[1:]
        unix_script_path = os.path.join(scripts_dir, custom_script_path)
        if not os.path.isfile(unix_script_path):
            err_msg = 'You provided custom action scripts\n'
            err_msg += 'However, this file is missing :{0}'
            err_msg = err_msg.format(unix_script_path)
            raise ValueError(Utilities.print_box_msg(err_msg, 'x'))
        # replace / with _ to make it a unique file name
        new_file_name = custom_script_path.replace('/', '_')
        final_save_path = os.path.join(self.cfg_mgr.files, new_file_name)
        # copy file to files dir to save to git
        shutil.copy(unix_script_path, final_save_path)
        if rule.is_hive_script:
            with open(final_save_path, 'r+') as file_h:
                current_content = file_h.read()
                file_h.seek(0, 0)
                new_content = self.gen_hive_connect_settings() + '\n' + \
                    current_content
                file_h.write(new_content)
        self.custom_action_scripts.append(new_file_name)
        return new_file_name

    def gen_hive_connect_settings(self):
        """Generates Hive connection settings that can be used at the top
        of hive scripts
        """
        hive_options = 'SET mapred.job.queue.name={queue_name};\n'
        hive_options += 'SET mapreduce.map.memory.mb=8000;\n'
        hive_options += 'SET mapreduce.reduce.memory.mb=16000;\n'
        hive_options += 'SET hive.exec.dynamic.partition.mode=nonstrict;\n'
        hive_options += 'SET hive.exec.dynamic.partition=true;\n'
        hive_options += 'SET hive.warehouse.subdir.inherit.perms=true;\n\n'
        hive_options += "SET parquet.block.size=268435456; \n"
        hive_options += "SET dfs.blocksize=1000000000; \n"
        hive_options += "SET mapred.min.split.size=1000000000; \n\n"
        hive_options = hive_options.format(queue_name=self.cfg_mgr.queue_name)
        return hive_options

    def gen_hive_action(self, action_name, source_dir, hql_file, ok_to,
                        error_to):
        """Returns hive action that executes the hql generated"""
        params = {'cfg_mgr': self.cfg_mgr, 'action_type': 'hive',
                  'name': action_name,
                  'ok': ok_to, 'error': error_to,
                  'script': '${nameNode}' + source_dir + '/{hql}'.format(
                      hql=hql_file)}
        return Hive(**params)

    def gen_shell_action(self, action_name, shell_dir, script, ok_to,
                         error_to):
        """Returns shell action that executes the script generated"""
        params = {'cfg_mgr': self.cfg_mgr, 'action_type': 'shell',
                  'name': action_name,
                  'ok': ok_to, 'error': error_to, 'execute': script,
                  'env_var': ['HADOOP_CONF_DIR=/etc/hadoop/conf'],
                  'file': ['{dir}/{script}#{script}'.format(dir=shell_dir,
                                                            script=script)]}
        return Shell(**params)

    def gen_impala_action(self, action_name, impala_dir, script, ok, error):
        """Returns impala action that executes the script generated"""
        params = {
            'cfg_mgr': self.cfg_mgr, 'action_type': 'impala',
            'name': action_name, 'ok': ok, 'error': error,
            'execute': 'oozie-impala.sh',
            'env_var': ['impala_daemon=${impala_daemon}',
                        'impala_file={script}'.format(script=script)],
            'file': ['${nameNode}' + impala_dir +
                     '/{script}'.format(script=script),
                     '/user/dev/lib/ingest/oozie-impala.sh#oozie-impala.sh',
                     '/user/dev/oozie.tab#oozie.tab']}
        return Impala(**params)
