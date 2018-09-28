"""Create oozie workflows."""
import collections
from pkg_resources import resource_filename
from mako.template import Template
from ibis.custom_logging import get_logger
from ibis.inventor.action_builder import ActionBuilder
from ibis.utilities.utilities import *
from ibis.inventor.dsl_parser import DSLParser


class WorkflowGenerator(object):
    """Generates a workflow given a list[list[table]].
    Uses templates to generate workflow and metadata and values
    from a table properties to populate template values.
    """

    def __init__(self, workflow_name, cfg_mgr):
        self.cfg_mgr = cfg_mgr
        self.action_builder = ActionBuilder(cfg_mgr)
        self.utilities = Utilities(cfg_mgr)
        self.action_builder.workflowName = workflow_name
        self.logger = get_logger(self.cfg_mgr)
        # Workflow being written
        self.wf_file_path = os.path.join(self.cfg_mgr.files,
                                         workflow_name + '.xml')
        self.file_out = open(self.wf_file_path, "wb")
        self.workflow_name = workflow_name
        self.workflow_started = False
        self.sub_workflow_template = ''
        self.workflows_chunks = []  # used for subworkflow generation

    def sort_table_prop_by_load(self, tables):
        """Uses a map with full_table_name: table object to created a
        sorted dictionary with keys 100, 010, 001
        Args:
            tables: List[ibis.model.table.ItTable]
        Returns:
            load_dict: OrderedDict - dict{'weight': [ibis.model.table.ItTable]}
        """
        load_dict = {'100': [], '010': [], '001': []}
        for table in tables:
            if table.is_small:
                light = load_dict['100']
                light.append(table)
                load_dict['100'] = light
            elif table.is_medium:
                medium = load_dict['010']
                medium.append(table)
                load_dict['010'] = medium
            elif table.is_heavy:
                heavy = load_dict['001']
                heavy.append(table)
                load_dict['001'] = heavy
            else:
                err_msg = "Unknown key '{load}' in load column"
                err_msg = err_msg.format(load=table.load)
                raise ValueError(Utilities.print_box_msg(err_msg, 'x'))

        for load_val in load_dict.keys():
            load_list = load_dict[load_val]
            load_list = sorted(
                load_list, key=lambda x: x.table_name, reverse=False)
            load_dict[load_val] = load_list

        return collections.OrderedDict(sorted(load_dict.items()))

    def gen_pipelines(self, load_dict, is_sub=False):
        """Given a dictionary containing load values :
        list of table objects, generate pipeline list for workflow
        Args:
            load_dict: OrderedDict - dict{'weight': [ibis.model.table.ItTable]}
            is_sub: boolean for if subworkflow
        Returns:
            pipelines: List[List[ibis.model.table.ItTable]]
        """
        l_max = 4  # max pipeline list size for lightweight tables
        m_max = 2  # max pipeline list size for medium and heavy tables
        h_max = 1 if is_sub else 2
        light_tables = []
        med_tables = []
        heavy_tables = []

        for key in load_dict:
            if key == '100':
                light_tables = load_dict[key]
            elif key == '010':
                med_tables = load_dict[key]
            elif key == '001':
                heavy_tables = load_dict[key]
            else:
                err_msg = 'Unknown key {key} provided in load dictionary'
                err_msg = err_msg.format(key)
                self.logger.error(err_msg)
        pipelines = []
        # Chunk tables, add to pipelines list, remove table from table list
        if light_tables:
            chunks = list(self.list_chunks(light_tables, l_max))
            # Number of splits/groupings
            for chunk in chunks:
                if len(chunk) % l_max == 0:
                    # Optimal grouping of 4 light tables
                    pipelines.append(chunk)
                    for tbl in chunk:
                        light_tables.remove(tbl)
        if med_tables:
            chunks = list(self.list_chunks(med_tables, m_max))
            for chunk in chunks:
                if len(chunk) % m_max == 0:
                    pipelines.append(chunk)
                    for tbl in chunk:
                        med_tables.remove(tbl)
        if heavy_tables:
            chunks = list(self.list_chunks(heavy_tables, h_max))
            for chunk in chunks:
                if len(chunk) % h_max == 0:
                    pipelines.append(chunk)
                    for tbl in chunk:
                        heavy_tables.remove(tbl)

        # Check if there are any tables left due to
        # an odd number of tables, pair up with other load tables
        if light_tables:
            chunk = light_tables
            if len(chunk) == 1:
                # Pair light with a med or heavy
                if med_tables:
                    chunk.append(med_tables[0])
                    med_tables.remove(med_tables[0])
                elif heavy_tables:
                    chunk.append(heavy_tables[0])
                    heavy_tables.remove(heavy_tables[0])
            pipelines.append(chunk)
        if med_tables:
            chunk = med_tables
            if heavy_tables:
                chunk.append(heavy_tables[0])
                heavy_tables.remove(heavy_tables[0])
            pipelines.append(chunk)
        if heavy_tables:
            pipelines.append(heavy_tables)

        # Sort each pipeline by table name for easier validation
        for index, pipeline in enumerate(pipelines):
            pipeline = sorted(pipeline, key=lambda x: x.table_name,
                              reverse=False)
            pipelines[index] = pipeline
        return pipelines

    def gen_workflow_start(self):
        """Generates the start of the workflow according to
        self.cfg_mgr.start_template template"""
        template = Template(filename=self.cfg_mgr.start_template,
                            format_exceptions=True)
        xml = template.render(workflowName=self.workflow_name)
        self.file_out.write(xml)

    def gen_workflow_end(self):
        """Generates the end of the workflow with the appropriate nodes
        according to the self.cfg_mgr.end_template template"""
        template = Template(filename=self.cfg_mgr.end_template,
                            format_exceptions=True)
        end_xml = template.render()
        self.file_out.write('\n')
        self.file_out.write(end_xml)
        self.file_out.close()

    def gen_oozie_cb_action(self):
        """Generate oozie checks and balances action to workflow"""
        template_file = resource_filename('resources.templates',
                                          'oozie_cb.xml.mako')
        template = Template(filename=template_file, format_exceptions=True)
        oozie_cb_xml = template.render(
            hdfs_ingest_path=self.action_builder.get_hdfs_files_path(),
            workflowName=self.workflow_name)
        self.file_out.write('\n')
        self.file_out.write(oozie_cb_xml)

    def gen_workflow_export_end(self):
        """Generates the end of the workflow with the appropriate nodes
        according to the self.cfg_mgr.end_template template"""
        template = Template(filename=self.cfg_mgr.export_end_template,
                            format_exceptions=True)
        xml = template.render(
            hdfs_export_path=self.action_builder.get_hdfs_files_path(),
            workflowName=self.workflow_name)
        self.file_out.write('\n')
        self.file_out.write(xml)
        self.file_out.close()

    def gen_full_ingest_actions(self, table, ok_to_action):
        """Generates the workflow actions required for a table
        ingestion and write out to file.
        Args:
            table: instance of ibis.model.table.ItTable
            ok_to_action: {sqoop_to: 'value', end_to: 'value'}
        """
        sqoop_to = ok_to_action['sqoop_to']
        end_to = ok_to_action['end_to']
        xml = ''
        if table.has_auth_info():
            self.logger.info(
                "Workflow generation started: {0}".format(table.full_sql_name))
            xml = self.action_builder.gen_full_table_ingest(
                table, sqoop_to, end_to)
        else:
            err_msg = 'Table has no proper auth info\n'
            err_msg += ("domain: '{0}'\nfull-table-name: '{1}'\n"
                        "username: '{2}'\npassword: '{3}'\njdbcurl: '{4}'")
            err_msg = err_msg.format(table.domain, table.full_sql_name,
                                     table.username, table.password_file,
                                     table.jdbcurl)
            raise ValueError(Utilities.print_box_msg(err_msg, 'x'))
        self.file_out.write(xml)

    def list_chunks(self, my_list, num):
        """Gets num sized chunks from list"""
        for i in xrange(0, len(my_list), num):
            yield my_list[i: i + num]

    def set_workflow_start(self, pipelines, is_sub=False):
        """Sets the first action for the workflow"""
        pipe = pipelines[0]
        contains_heavy = False

        if not self.workflow_started:
            # Workflow actions do not have a starting point
            for table in pipe:
                if table.load == '001':
                    # Heavy
                    contains_heavy = True
            # Only one table in first pipeline or staggering heavy tables
            if len(pipe) == 1 or contains_heavy:
                if not is_sub:
                    # table workflow
                    table = pipe[0]
                    action_name = table.table_name + '_import_prep'
                    action_name = Utilities.replace_special_chars(action_name)
                    self.file_out.write('\t<start to="{0}"/>\n'.format(
                        action_name))
                else:
                    self.file_out.write('\t<start to="job_0"/>\n')
            elif not contains_heavy:
                if not is_sub:
                    self.file_out.write(
                        '\t<start to="{0}"/>\n'.format('pipeline0'))
                else:
                    self.file_out.write('\t<start to="job_0"/>\n')
            self.workflow_started = True

    def get_pipeline_weight(self, pipeline):
        """Checks the tables in a pipeline to see what weights it contains"""
        is_light = False
        is_medium = False
        is_heavy = False
        for table in pipeline:
            load = table.load
            if load == '100':
                is_light = True
            elif load == '010':
                is_medium = True
            elif load == '001':
                is_heavy = True
        return is_light, is_medium, is_heavy

    def _gen_light_action(self, pipeline, pipeline_name, is_sub=False):
        """Generates the actions for a light weight and
         medium weight pipeline"""
        paths = []
        for table in pipeline:
            paths.append(table.table_name)
        fork_xml = self.action_builder.gen_fork_xml(pipeline_name, paths,
                                                    'concurrent', is_sub)
        self.file_out.write(fork_xml)
        for table in pipeline:
            if not is_sub:
                self.gen_full_ingest_actions(table, {
                    'sqoop_to': table.table_name + '_avro',
                    'end_to': pipeline_name + '_join'})
            else:
                ok_to = pipeline_name + '_join'
                wf_name = self._filter_wf(table)
                self._write_subwf(table.table_name, wf_name, ok_to, ok_to)

    def _gen_heavy_action(self, pipeline, pipeline_name, is_sub=False):
        """Generates staggered actions for a pipeline containing a heavy table.
        Expects a pipeline with a pair of tables one being heavy"""
        paths = [table.table_name for table in pipeline]
        fork_xml = self.action_builder.gen_fork_xml(pipeline_name, paths,
                                                    'staggered', is_sub)
        self.file_out.write(fork_xml)

        if not is_sub:
            self.gen_full_ingest_actions(
                pipeline[0], {'sqoop_to': pipeline_name,
                              'end_to': pipeline_name + '_join'})
            self.gen_full_ingest_actions(
                pipeline[1], {'sqoop_to': pipeline[1].table_name + '_avro',
                              'end_to': pipeline_name + '_join'})
        else:
            ok_to = pipeline_name + '_join'

            wf_name = self._filter_wf(pipeline[0])
            self._write_subwf(pipeline[0].db_table_name, wf_name, ok_to, ok_to)

            wf_name = self._filter_wf(pipeline[1])
            self._write_subwf(pipeline[1].db_table_name, wf_name, ok_to, ok_to)

    def to_pipeline(self, to_pipeline, cur_index):
        """Returns the ok to value for going from one pipeline to the next"""
        ok_to = ''
        if len(to_pipeline) > 1:
            # more than one table in pipeline
            ok_to = 'pipeline' + str(cur_index + 1)
        else:
            table = to_pipeline[0]
            ok_to = table.table_name + '_import_prep'
        return ok_to

    def gen_oracle_export(self, source_table_name, source_database_name,
                          source_dir, jdbc_url,
                          update_key, target_table_name, target_database_name,
                          user_name,
                          password_alias):
        self.gen_workflow_start()
        self.file_out.write('\n')
        self.file_out.write(
            '\t<start to=\"' + source_table_name + '_genhql\"/>')
        export_xml = self.action_builder.gen_oracle_table_export(
            source_table_name, source_database_name, source_dir, jdbc_url,
            update_key,
            target_table_name, target_database_name, user_name, password_alias)
        self.file_out.write(export_xml)
        self.gen_oozie_cb_action()
        self.gen_workflow_end()

    def gen_teradata_export(self, source_table_name, source_database_name,
                            source_dir, jdbc_url, target_table_name,
                            target_database_name, user_name, password_alias):
        self.gen_workflow_start()
        self.file_out.write('\n')
        self.file_out.write('\t<start to=\"' + source_table_name +
                            '_genhql\"/>')
        export_xml = self.action_builder.gen_teradata_table_export(
            source_table_name, source_database_name, source_dir, jdbc_url,
            target_table_name, target_database_name, user_name, password_alias)
        self.file_out.write(export_xml)
        self.gen_oozie_cb_action()
        self.gen_workflow_end()

    def gen_database_export(self, tables):
        self.gen_workflow_start()
        self.file_out.write('\n')
        self.file_out.write(
            '\t<start to=\"' + tables[0].table_name + '_export_prep\"/>')
        export_xml = self.action_builder.gen_database_table_export(tables)
        self.file_out.write(export_xml)
        self.gen_workflow_export_end()

    def gen_workflow_from_pipelines(self, pipelines, is_sub=False):
        """Generates a workflow from a list[list[table]],
        Each index of the main list is treated as a pipeline
        Args:
            pipelines: List[List[ibis.model.table.ItTable]]
            is_sub: boolean if it is for subworkflow
        """
        self.gen_workflow_start()
        self.file_out.write('\n')
        self.set_workflow_start(pipelines, is_sub)
        end_to = 'end'
        oozie_cb_ok_to = 'oozie_cb_ok'

        if is_sub:
            self.sub_workflow_template = self.utilities.get_lines_from_file(
                self.cfg_mgr.sub_workflow_template)

        for i, pipeline in enumerate(pipelines):
            if is_sub:
                pipeline_name = 'job_' + str(i)
            else:
                pipeline_name = 'pipeline' + str(i)

            if len(pipeline) == 1 and len(pipelines) == 1:
                # Only one table requested in pipelines = [[table]]
                table = pipeline[0]
                if len(pipelines) - 1 == i:
                    # Only pipeline in pipelines list
                    if not is_sub:
                        self.gen_full_ingest_actions(table, {
                            'sqoop_to': table.table_name + '_avro',
                            'end_to': oozie_cb_ok_to})
                    else:
                        wf_name = self._filter_wf(table)
                        self._write_subwf('job_{id}'.format(id=i),
                                          wf_name, end_to)
            else:
                # More than one table in pipeline
                if len(pipeline) > 1:
                    (is_light, is_medium, is_heavy) = self.get_pipeline_weight(
                        pipeline)

                    if (is_light or is_medium) and not is_heavy:
                        # All tables in current pipeline light weight
                        self._gen_light_action(pipeline, pipeline_name, is_sub)
                    elif is_heavy:
                        self._gen_heavy_action(pipeline, pipeline_name, is_sub)

                    if len(pipelines) - 1 == i:
                        # Last pipeline
                        if is_sub:
                            join_xml = self.action_builder.gen_join_xml(
                                pipeline_name + '_join', end_to)
                        else:
                            join_xml = self.action_builder.gen_join_xml(
                                pipeline_name + '_join', oozie_cb_ok_to)
                        self.file_out.write(join_xml)
                    else:
                        if not is_sub:
                            next_pipeline = self.to_pipeline(pipelines[i + 1],
                                                             i)
                        else:
                            next_pipeline = 'job_' + str(i + 1)
                        is_light, is_medium, is_heavy = \
                            self.get_pipeline_weight(pipelines[i + 1])
                        if (not is_sub) and is_heavy:
                            pipeline = pipelines[i + 1]
                            table = pipeline[0]
                            next_import = table.table_name + '_import_prep'
                            join_xml = self.action_builder.gen_join_xml(
                                pipeline_name + '_join',
                                next_import)
                            self.file_out.write(join_xml)
                        else:
                            join_xml = self.action_builder.gen_join_xml(
                                pipeline_name + '_join',
                                next_pipeline)
                            self.file_out.write(join_xml)
                elif len(pipeline) == 1:
                    # One table in pipeline
                    table = pipeline[0]
                    if len(pipelines) - 1 == i:
                        if not is_sub:
                            self.gen_full_ingest_actions(
                                table, {'sqoop_to': table.table_name + '_avro',
                                        'end_to': oozie_cb_ok_to})
                        else:
                            wf_name = self._filter_wf(table)
                            self._write_subwf('job_{id}'.format(id=i),
                                              wf_name, end_to)
                    else:
                        if not is_sub:
                            next_pipeline = self.to_pipeline(pipelines[i + 1],
                                                             i)
                            self.gen_full_ingest_actions(
                                table, {'sqoop_to': table.table_name + '_avro',
                                        'end_to': next_pipeline})
                        else:
                            wf_name = self._filter_wf(table)
                            self._write_subwf('job_{id}'.format(id=i),
                                              wf_name, 'job_' + str(i + 1))
        if not is_sub:
            self.gen_oozie_cb_action()
        self.gen_workflow_end()

    def _write_subwf(self, action_name, workflow_file_name,
                     ok_to, end_to=None):
        """Write sub workflow content to file_out"""
        workflow_path = self.cfg_mgr.oozie_workspace + workflow_file_name + \
            '.xml'
        props = {'action_name': action_name, 'xml_file': workflow_path,
                 'ok': ok_to}
        if end_to:
            props['end_to'] = end_to

        subwf_xml = ''
        for line in self.sub_workflow_template:
            _str = string.Formatter().vformat(line, (), SafeDict(**props))
            subwf_xml += _str
        self.file_out.write(subwf_xml)

    def _filter_wf(self, table):
        """Fetch workflow name based on table name
        Args:
            table: ibis.model.table.ItTable
        Returns:
            wf_name: workflow name
        """
        wf_name = ''
        # self.workflows_chunks -
        # List[List[ibis.model.table.ItTable, xml file name]]
        for table_and_file_name in self.workflows_chunks:
            tbl = table_and_file_name[0]
            if tbl.table_id == table.table_id:
                wf_name = table_and_file_name[1]
                break
        return wf_name

    def generate(self, tables):
        """Main method for generating a full workflow from a list containing
        the map values of a user table properties
        Args:
            tables: List of ibis.model.table.ItTable
        """
        # Sorts table properties by load
        loads_map = self.sort_table_prop_by_load(tables)
        # Generates pipeline list
        pipelines = self.gen_pipelines(loads_map)
        self.gen_workflow_from_pipelines(pipelines)

    def generate_subworkflow(self, workflow_list):
        """Generates a workflow with subworkflows"""
        self.gen_workflow_start()
        self.file_out.write('\n')
        sub_workflow_template = self.utilities.get_lines_from_file(
            self.cfg_mgr.sub_workflow_template)
        self.file_out.write('    <start to="job_0"/>\n')
        for i, workflow in enumerate(workflow_list):
            if i != len(workflow_list) - 1:
                props = {'action_name': 'job_{id}'.format(id=i),
                         'xml_file': workflow,
                         'ok': 'job_{id}'.format(id=i + 1)}
            else:
                props = {'action_name': 'job_{id}'.format(id=i),
                         'xml_file': workflow, 'ok': 'oozie_cb_ok'}
            for line in sub_workflow_template:
                self.file_out.write(
                    string.Formatter().vformat(line, (), SafeDict(**props)))
            self.file_out.write('\n')
        self.gen_workflow_end()

    def generate_td_export(self, export_dict):
        """Generates a workflow to export a table to teradata
        :param export_dict A dictionary containing values to replace
        template parameters.
        Requires action_name (name of action), db (db to export to),
        table (table to export to),
        export_dir (hdfs directory to export), ok (ok action to)"""
        self.gen_workflow_start()
        export_template = self.utilities.get_lines_from_file(
            self.cfg_mgr.export_to_td_template)
        for line in export_template:
            self.file_out.write(
                string.Formatter().vformat(line, (), SafeDict(**export_dict)))
        self.file_out.write('\n')
        self.gen_oozie_cb_action()
        self.gen_workflow_end()

    def generate_incremental(self, table):
        """Generate incremental xml if table has incremental check column
        Args:
            table: instance of ibis.model.table.ItTable
        """
        self.gen_workflow_start()
        self.file_out.write('\n')

        if not table.check_column:
            return
        self.action_builder.it_table = table
        self.action_builder.error_to_action = 'oozie_cb_fail'
        col_types = self.action_builder.get_col_types(table)
        column_names = self.action_builder.sqooper.get_column_names(col_types)
        ts_columns = self.action_builder.get_ts_columns(col_types,
                                                        table.jdbcurl)
        # check if column is valid
        if table.check_column not in column_names:
            err_msg = ("Not a valid column for check_column: '{0}'\n\n"
                       "Possible valid columns: '{1}'")
            err_msg = err_msg.format(table.check_column,
                                     "', '".join(column_names))
            raise ValueError(Utilities.print_box_msg(err_msg, 'x'))

        is_ts_column = False
        for column in ts_columns:
            if column == table.check_column:
                is_ts_column = True
                break

        views_xml = ''
        error_to = 'oozie_cb_fail'
        db_table_name = Utilities.replace_special_chars(table.db_table_name)
        table.clean_db_table_name = Utilities.replace_special_chars(
            table.db_table_name)
        import_ok_to = 'check_if_empty_' + db_table_name
        merge_ok_to = db_table_name + '_refresh'
        jceks, password = table.split_password_file
        last_value = "${{wf:actionData('import_prep_{0}')['sqoopLstUpd']}}"
        last_value = last_value.format(db_table_name)

        where_clause = None
        if is_ts_column:
            incremental_mode = 'lastmodified'
            if table.is_oracle:
                # See here: https://sqoop.apache.org/docs/1.4.5/
                # SqoopUserGuide.html#
                # _oraoop_table_import_where_clause_location
                where_clause = ("{0} > to_timestamp('{1}', "
                                "'YYYY-MM-DD HH24:MI:SS.FF9')")
                where_clause = where_clause.format(
                    table.check_column.upper(), last_value)
            elif table.is_teradata:
                # This needs to be a specific Teradata query
                where_clause = "{0} > '{1}'"
                where_clause = where_clause.format(
                    table.check_column.upper(), last_value)
            elif table.is_sqlserver:
                # Info here:
                # http://stackoverflow.com/questions/10643379/how-do-
                # i-query-for-all-dates-greater-than-a-
                # certain-date-in-sql-server
                # http://stackoverflow.com/questions/1334143/
                # datetime2-vs-datetime-in-sql-server
                where_clause = "{0} > convert(datetime2, '{1}')"
                where_clause = where_clause.format(
                    table.check_column, last_value)
            elif table.is_db2:
                # Info here:
                # http://datawarehouse.ittoolbox.com/groups/technical-
                # functional/abinitio-l/timestamp-comparison-in-
                # select-query-for-db2-on-input-table-732527
                where_clause = "timestamp({0}) > timestamp('{1}')"
                where_clause = where_clause.format(
                    table.check_column.upper(), last_value)
            elif table.is_postgresql:
                where_clause = "{0} > '{1}'"
                where_clause = where_clause.format(
                    table.check_column, last_value)
            elif table.is_mysql:
                where_clause = "{0} > '{1}'"
                where_clause = where_clause.format(
                    table.check_column, last_value)
            else:
                raise ValueError("Invalid source provided")
        else:
            incremental_mode = 'append'

        incremental = {
            'check_column': table.check_column,
            'incremental': incremental_mode,
            'last_value': last_value
        }

        sqoop_action = self.action_builder.gen_import_action(
            'incr_import_' + db_table_name,
            incremental=incremental, sqoop_where_query=where_clause)
        sqoop_action.ok = import_ok_to
        sqoop_xml = sqoop_action.generate_action()

        self.action_builder.workflowName = self.workflow_name
        qa_action = self.action_builder.gen_quality_assurance_action(
            'incr_quality_assurance_' + db_table_name,
            "incremental")
        qa_action.ok = 'incr_merge_partition_' + db_table_name
        qa_xml = qa_action.generate_action()

        views_action = self.action_builder.gen_create_views_action(
            db_table_name + '_views')
        views_action.ok = db_table_name + '_refresh'
        views_xml = views_action.generate_action()
        merge_ok_to = db_table_name + '_views'

        template_file = resource_filename('resources.templates',
                                          'incr_wf.xml.mako')
        template = Template(filename=template_file, format_exceptions=True)
        incr_xml = template.render(
            it_table_obj=table, sqoop_xml=sqoop_xml, qa_xml=qa_xml,
            jceks=jceks, password_alias=password,
            it_table_host=self.cfg_mgr.workflow_host,
            it_table=self.cfg_mgr.it_table,
            error_to=error_to, incremental_mode=incremental_mode,
            hdfs_ingest_path=self.action_builder.get_hdfs_files_path(),
            views_xml=views_xml, merge_ok_to=merge_ok_to)

        refresh_action = self.action_builder.gen_refresh(
            db_table_name + '_refresh')
        refresh_action.ok = 'oozie_cb_ok'
        refresh_xml = refresh_action.generate_action()
        incr_xml += refresh_xml

        self.file_out.write(incr_xml + '\n')
        self.gen_oozie_cb_action()
        self.gen_workflow_end()
        self.file_out.close()
        self.logger.info(
            'Generated incremental workflow: {0}'.format(self.wf_file_path))
        msg = 'Workflow {0}.xml generated successfully.'.format(
            self.workflow_name)
        self.logger.info(msg)
        return True

    def gen_kite_ingest_workflow(self, request_list):
        kite_xml = ''
        self.gen_workflow_start()
        self.file_out.write('\n')
        for i, table in enumerate(request_list):
            if i == 0:
                self.file_out.write(
                    '\t<start to=\"' +
                    table['source_table_name'] + '_kite_ingest\"/>')
            if i == len(request_list) - 1:
                ok_to = 'oozie_cb_ok'
            else:
                ok_to = request_list[i+1]['source_table_name'] + '_kite_ingest'
            kite_xml += self.action_builder.gen_kite_ingest(
                table['source_table_name'], table['source_database_name'],
                table['hdfs_loc'], ok_to, 'oozie_cb_fail')
        self.file_out.write(kite_xml)
        self.gen_oozie_cb_action()
        self.gen_workflow_end()

    def gen_config_workflow(self, config_file_name, scripts_dir):
        """Generates config based workflows
        Args:
            config_file_name: file name
        """
        dslparser = DSLParser(self.cfg_mgr, [], scripts_dir)
        rules = dslparser.get_custom_rules(config_file_name)
        actions = []
        error_to_action = 'kill'
        workflow_xml = ''
        action_name_prefix = config_file_name.replace('.', '_')

        for index, rule in enumerate(rules):
            if rule.action_id == 'hive_script':
                action_name = action_name_prefix + '_hive_' + str(index + 1)
                script_name = self.action_builder.gen_custom_workflow_scripts(
                    rule, dslparser.scripts_dir)
                hdfs_path = self.cfg_mgr.custom_scripts_hql
                hive_action = self.action_builder.gen_hive_action(
                    action_name, hdfs_path, script_name, '', error_to_action)
                actions.append(hive_action)
                self.logger.info(
                    'Adding action script: {0}'.format(script_name))
            elif rule.action_id == 'shell_script':
                action_name = action_name_prefix + '_shell_' + str(index + 1)
                script_name = self.action_builder.gen_custom_workflow_scripts(
                    rule, dslparser.scripts_dir)
                hdfs_path = self.cfg_mgr.custom_scripts_shell
                shell_action = self.action_builder.gen_shell_action(
                    action_name, hdfs_path, script_name, '', error_to_action)
                actions.append(shell_action)
                self.logger.info(
                    'Adding action script: {0}'.format(script_name))

        for current_action, next_action in Utilities.pairwise(actions):
            current_action.ok = next_action.get_name()
            workflow_xml += current_action.generate_action()
        last_action = actions[-1]
        last_action.ok = 'end'
        workflow_xml += last_action.generate_action()
        self.gen_workflow_start()
        start_node = '\n\t<start to="{start}"/>\n'.format(
            start=actions[0].name)
        self.file_out.write(start_node)
        self.file_out.write(workflow_xml)
        self.gen_workflow_end()
        msg = 'Generated custom workflow: {0}.xml'.format(self.workflow_name)
        self.logger.info(msg)
        return workflow_xml
