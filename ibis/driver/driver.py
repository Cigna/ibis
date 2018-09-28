"""Driver module."""
import getpass
import json
import os
import subprocess
import time
import traceback
import prettytable

from ibis.custom_logging import get_logger
from ibis.inventor.workflow_generator import WorkflowGenerator
from ibis.inventory.cb_inventory import CheckBalancesInventory
from ibis.inventory.esp_ids_inventory import ESPInventory
from ibis.inventory.export_it_inventory import ExportITInventory
from ibis.inventory.inventory import Inventory
from ibis.inventory.it_inventory import ITInventory
from ibis.inventory.perf_inventory import PerfInventory
from ibis.inventory.request_inventory import RequestInventory, \
    REQUIRED_FIELDS, OPTIONAL_FIELDS, REQUIRED_FIELDS_EXPORT,\
    OPTIONAL_FIELDS_EXPORT
from ibis.model.exporttable import ItTableExport
from ibis.model.table import ItTable
from ibis.utilities.file_parser import parse_file_by_sections
from ibis.utilities.it_table_generation import create, Get_Auto_Split
from ibis.utilities.run_parallel import SqoopCacheManager, \
    DryRunWorkflowManager
from ibis.utilities.sqoop_auth_check import AuthTest
from ibis.utilities.utilities import Utilities, WorkflowTablesMapper
from ibis.utilities.vizoozie import VizOozie


class Driver(object):

    """Drive the ibis egg by providing methods to be accessed via the CLI."""

    def __init__(self, cfg_mgr):
        """Initialize the driver class.
        :param cfg_mgr ConfigManager object
        """
        self.cfg_mgr = cfg_mgr
        self.workflow_gen = None
        self.inventory = Inventory(self.cfg_mgr)
        self.perf_inventory = PerfInventory(self.cfg_mgr)
        self.req_inventory = RequestInventory(self.cfg_mgr)
        self.it_inventory = ITInventory(self.cfg_mgr)
        self.export_it_inventory = ExportITInventory(self.cfg_mgr)
        self.esp_inventory = ESPInventory(self.cfg_mgr)
        self.utilities = Utilities(self.cfg_mgr)
        self.cb_inventory = CheckBalancesInventory(self.cfg_mgr)
        self.vizoozie = VizOozie(self.cfg_mgr)
        self.sqoop_cache = SqoopCacheManager(self.cfg_mgr)
        self.dryrun_parallel = DryRunWorkflowManager(self.cfg_mgr)
        self.table_workflows = {}
        self.logger = get_logger(self.cfg_mgr)
        self.it_table_generation = Get_Auto_Split(self.cfg_mgr)

    def submit_it_file(self, request_file):
        """Used to submit a file containing [Request] headers
        with appropriate values.
        Update/Add those records to the it_table
        Args:
            request_file: Instance of open(). Text file containing
            values to insert into it_table, made up of sections of [Request]
            Returns(str): msg
        """
        request_tables, msg = self.req_inventory.parse_file(request_file)
        return self.update_it_table(request_tables, msg)

    def submit_it_file_export(self, request_file):
        """Used to submit a file containing [Request] headers
        with appropriate values.
        Update/Add those records to the it_table
        Args:
            request_file: Instance of open(). Text file containing
            values to insert into it_table, made up of sections of [Request]
            Returns(str): msg
        """
        request_tables, msg = \
            self.req_inventory.parse_file_export(request_file)
        return self.update_it_table_export(request_tables, msg)

    def determine_auto_domain(self, table):
        """If domain is not present, then it is set to database_i
        Args:
            table: instance of ibis.model.table.ItTable
        """
        if table.domain == '':
            table.domain = table.database + self.cfg_mgr.domain_suffix

    def add_env_to_domain(self, table):
        """adding env to domain if user provides it in request-file
        Args:
            table: instance of ibis.model.table.ItTable
        """
        default_val = self.cfg_mgr.default_db_env.lower()
        if table.db_env != default_val:
            if table.db_env + '_' not in table.domain:
                # do not update everytime repeating it
                table.domain = table.db_env + '_' + table.domain

    def update_it_table(self, request_tables, msg):
        """Updates or inserts new it table row
        Args:
            request_tables: list(ibis.inventory.request_inventory.Request)
            msg: string. error message if any
        """
        if not request_tables:
            return msg

        tables = self.it_inventory.parse_requests(request_tables)

        for modified_table_obj in tables:
            new_table = False
            table_dict = self.it_inventory.get_table_mapping(
                modified_table_obj.database, modified_table_obj.table_name,
                modified_table_obj.db_env)

            if table_dict:
                existing_table_obj = ItTable(table_dict, self.cfg_mgr)
            else:
                new_table = True
                _table = {'source_table_name': modified_table_obj.table_name,
                          'source_database_name': modified_table_obj.database,
                          'db_env': modified_table_obj.db_env}
                existing_table_obj = ItTable(_table, self.cfg_mgr)
            modified_columns, modified_values = \
                self.get_table_diff(modified_table_obj, existing_table_obj)

            self.determine_auto_domain(existing_table_obj)
            self.add_env_to_domain(existing_table_obj)

            split_by = None
            if modified_table_obj.split_by == 'null':
                split_by = "no-split"
            elif (not bool(existing_table_obj.split_by) and not
                  existing_table_obj.is_oracle and
                  bool(existing_table_obj.jdbcurl)):
                split_by = self.it_table_generation.get_split_by_column(
                    existing_table_obj)
            else:
                self.logger.info('No split by required')

            if split_by is not None:
                if split_by == "no-split":
                    existing_table_obj.mappers = 1
                    modified_columns.append('mappers')
                    modified_values.append(str(1))
                    self.logger.info('Modified mappers to 1')
                else:
                    existing_table_obj.split_by = split_by
                    modified_columns.append('split_by')
                    modified_values.append(split_by)
                    self.logger.info('Modified split-by to:' + split_by)

            if modified_columns:
                self.logger.info('Modified columns: ' +
                                 ', '.join(modified_columns))
                self.logger.info('Modified values: ' +
                                 ', '.join(modified_values))
            if new_table:
                # insert
                success, msgg = self.it_inventory.insert(existing_table_obj)
                msg += '\n' + msgg
            else:
                # update
                if modified_columns:
                    success, msgg = self.it_inventory.update(
                        existing_table_obj)
                    if success:
                        self.logger.info('Updated: {0}'.format(
                            existing_table_obj.db_table_name))
                        msg += '\n' + msgg
                else:
                    self.logger.warning('Nothing to update: {0}'.format(
                        existing_table_obj.db_table_name))
        return msg

    def get_table_diff(self, modified_table_obj, existing_table_obj):
        """Return the diff between new table and existing table in
        it_table in column-values
        Args:
            modified_table_obj: instance of ibis.model.table.ItTable
            existing_table_obj: instance of ibis.model.table.ItTable
        Returns:
            modified_columns(field names), modified_values(field values)
        """
        modified_table = modified_table_obj.get_meta_dict()
        modified_columns = []
        modified_values = []
        for column, modified_val in modified_table.iteritems():
            old_val = ''
            if existing_table_obj and column not in REQUIRED_FIELDS:
                if column == 'source_schema_name':
                    old_val = existing_table_obj.schema
                elif column == 'sql_query':
                    old_val = existing_table_obj.query
                elif column == 'weight':
                    old_val = existing_table_obj.load_readable
                elif column == 'refresh_frequency':
                    old_val = existing_table_obj.frequency_readable
                elif column == 'db_username':
                    old_val = existing_table_obj.username
                elif column in OPTIONAL_FIELDS:
                    old_val = getattr(existing_table_obj, column)

            if column in REQUIRED_FIELDS:
                continue

            if modified_val == '':
                # key: in request file is ignored
                pass
            elif (str(modified_val) != str(old_val) and modified_val != '') \
                    or modified_val == 'null':
                if modified_val == 'null':
                    modified_val = ''

                if column == 'source_schema_name':
                    existing_table_obj.schema = modified_val
                elif column == 'sql_query':
                    existing_table_obj.query = modified_val
                elif column == 'weight':
                    existing_table_obj.load_readable = modified_val
                elif column == 'refresh_frequency':
                    existing_table_obj.frequency_readable = modified_val
                elif column == 'db_username':
                    existing_table_obj.username = modified_val
                else:
                    setattr(existing_table_obj, column, modified_val)

                modified_columns.append(column)
                modified_values.append(str(modified_val))
        return modified_columns, modified_values

    def _get_off_cycle_branch_name(self, file_name):
        """Returns git branch name for requested individual workflows
        Args:
            file_name: request file name
        """
        return file_name[:10] + "_" + time.strftime("%Y%m%d_%H_%M_%S")

    def _get_config_workflow_name(self, file_handler):
        """Returns workflow name
        Args:
            file_handler: instance of open()
        """
        file_name = os.path.abspath(file_handler.name).rsplit('/', 1)[-1]
        if '.' in file_name:
            file_name = file_name.rsplit('.', 1)[-2]
        file_name = getpass.getuser() + '_config_wf_' + file_name
        return file_name

    def _get_subworkflow_name_prefix(self, request_file):
        """Return workflow name derived from request file name
        Args:
            request_file: instance of open()
        """
        file_name = os.path.abspath(request_file.name).rsplit('/', 1)[-1]
        if '.' in file_name:
            file_name = file_name.rsplit('.', 1)[-2]
        file_name = getpass.getuser() + '_' + file_name
        return file_name

    def _get_workflow_name(self, request_file):
        """Return workflow name derived from request file name
        Args:
            request_file: instance of open()
        """
        db_env = self.cfg_mgr.default_db_env.lower()
        file_name = os.path.abspath(request_file.name).rsplit('/', 1)[-1]
        if '.' in file_name:
            file_name = file_name.rsplit('.', 1)[-2]
        file_name = db_env + '_' + getpass.getuser() + '_' + file_name
        return file_name

    def _get_workflow_name_table(self, table):
        """Return workflow name based on table name
        Args:
            table: ibis.model.table.ItTable
        """
        db_env = table.db_env
        file_nme = db_env + '_' + getpass.getuser() + '_' + \
            Utilities.replace_special_chars(table.db_table_name)
        return file_nme

    def _get_workflow_name_table_export(self, table):
        """Return workflow name based on table name
        Args:
            table: ibis.model.table.ItTable
        """
        db_env = table.db_env
        file_name = db_env + '_' + getpass.getuser() + '_' + \
            Utilities.replace_special_chars(table.db_table_name) + '_export'
        return file_name

    def _get_prod_table_workflow_name(self, table):
        """build workflow name for prod table workflow
        Args:
            table: ibis.model.table.ItTable
        """
        return Utilities.replace_special_chars(table.db_table_name)

    def _get_incr_workflow_name(self, table):
        """Return incremental workflow name
        Args:
            table: ibis.model.table.ItTable
        """
        incr_fname = table.db_env + '_' + getpass.getuser() + '_' + \
            'incr_' + Utilities.replace_special_chars(table.table_name)
        return incr_fname

    def build_table_wf_map(self, tables, wf_file_name,
                           incremental=False, sub_wf=False):
        """Build table and workflow file name mapping for printing
        Args:
            tables: List[ibis.model.table.ItTable]
            wf_file_name: workflow file name
            incremental: boolean if incremental
            sub_wf: boolean if workflow is a sub workflow
        """
        wf_file_name = wf_file_name + '.xml'
        view_tables = [', '.join(tbl.view_tables) for tbl in tables]
        view_table_names = ', '.join([_view.lower() for _view in view_tables])
        tables_id = ', '.join([tbl.table_id for tbl in tables])

        if tables_id in self.table_workflows.keys():
            if incremental:
                self.table_workflows[tables_id].incr_wf = wf_file_name
            else:
                self.table_workflows[tables_id].full_wf = wf_file_name
                if view_table_names:
                    self.table_workflows[tables_id].view_table_names = \
                        view_table_names

            if sub_wf:
                self.table_workflows[tables_id].is_subwf_wf = 'Yes'
        else:
            wf_table_obj = WorkflowTablesMapper(tables_id)
            if incremental:
                wf_table_obj.incr_wf = wf_file_name
            else:
                wf_table_obj.full_wf = wf_file_name
                if view_table_names:
                    wf_table_obj.view_table_names = view_table_names

            if sub_wf:
                wf_table_obj.is_subwf_wf = 'Yes'
            self.table_workflows[tables_id] = wf_table_obj

    def _print_table_wf_map(self):
        """Prints readable table"""
        print_obj = prettytable.PrettyTable([
            'Source Tables', 'Views - Hive Tables', 'Full Load Workflow',
            'Incremental Workflow', 'Grouped Workflow'])
        print_obj.hrules = prettytable.ALL
        print_obj.max_width = 80
        print_obj.sortby = 'Full Load Workflow'
        for each in self.table_workflows.values():
            print_obj.add_row([each.table_id, each.view_table_names,
                               each.full_wf, each.incr_wf, each.is_subwf_wf])
        msg = 'Caution: Run full load workflow prior to incremental workflow'
        self.logger.info('\n\n' + msg + '\n' + print_obj.get_string() + '\n')

    def gen_incr_workflow_files(self, available_tables):
        """Generates incremental workflow
        Args:
            available_tables: List[ibis.model.table.ItTable]
        Returns:
            list of generated files:
            .xml, _job.properties, .ksh, _props_job.xml
        """
        files_generated = []
        for table in available_tables:
            # generate incremental workflow
            incr_fname = self._get_incr_workflow_name(table)
            incr_wf = WorkflowGenerator(incr_fname, self.cfg_mgr)
            if incr_wf.generate_incremental(table):
                self.build_table_wf_map([table], incr_fname, True)
                self.utilities.gen_kornshell(incr_fname)
                self.utilities.gen_job_properties(incr_fname, sorted(
                    [table.table_name]), table.esp_appl_id)
                # self.utilities.dryrun_workflow(incr_fname)
                files_generated.extend([
                    incr_fname + '.xml', incr_fname + '_job.properties',
                    incr_fname + '.ksh', incr_fname + '_props_job.xml'])
        return files_generated

    def submit_request(self, request_file, no_git):
        """Used to submit a file containing a request table(s).
        Queries the it table to see which table(s) are available for
        workflow generation.
        Args:
            request_file: A text file contains sections of [Request]
        Returns:
            msg: A string
        """
        status = False
        requests, msg = self.req_inventory.parse_file(request_file)
        # update It-table before generating workflow
        self.update_it_table(requests, msg)
        if msg:
            # errors parsing file
            return status, msg

        tables, _, _ = \
            self.req_inventory.get_available_requests(requests)

        if tables:
            generated_workflows = []
            gen_files = []
            self.sqoop_cache.cache_ddl_queries(tables)
            self.sqoop_cache.cache_ddl_views(tables)

            for _, table in enumerate(tables):
                # Generate the workflow, job.properties, and kornshell
                wf_name = self._get_workflow_name_table(table)
                gen_files += self.gen_schedule_request([table], wf_name, None)
                generated_workflows.append([table, wf_name])
                msg = ('Workflow {file_name}.xml generated successfully. '
                       'Saved to {dir}')
                msg = msg.format(file_name=wf_name, dir=self.cfg_mgr.files)
                self.logger.info(msg)

            incr_files = self.gen_incr_workflow_files(tables)
            if incr_files:
                gen_files += incr_files

            workflows_chunks = self._group_workflows(generated_workflows)
            sub_wf_prefix = self._get_subworkflow_name_prefix(request_file)
            subwf_gen_files, _msg = self.gen_subworkflow(
                workflows_chunks, tables, sub_wf_prefix, None)

            gen_files += subwf_gen_files
            if not self.dryrun_parallel.run_all(gen_files):
                _err = Utilities.print_box_msg(
                    'Dryrun failed. Fix the workflow!', border_char='x')
                raise ValueError(_err)

            self._print_table_wf_map()
            status = True
        else:
            msg += 'Workflow not generated for request.\n'
            self.logger.warning(msg)
        if self.req_inventory.log_msg:
            self.logger.warning(Utilities.print_box_msg(
                self.req_inventory.log_msg, border_char='x'))
        return status, msg

    def insert_freq_ingest_driver(self, team_name, frequency, table, activate):
        """insert or update freq_ingest table
        Args:
        team_name - team DB name for updating the frequency and activator
        frequency - team run frequency
        table - team table name for updating the frequency and activator
        activate - activator status (yes/no)
        """
        if frequency is None and activate is None:
            raise ValueError("Either of frequency or activate column must "
                             "contain value")
        self.perf_inventory.insert_freq_ingest(team_name, frequency, table,
                                              activate)

    def wipe_perf_env_driver(self, db_name, reingest):
        """used to wipe tables from database
        Args:
            db_name - database name, reingest - boolean to reingest tables
        """
        db_name = db_name[0]
        if db_name.lower().endswith("_i") or db_name.lower() == 'ibis':
            raise ValueError("Cannot wipe Ibis database")
        domain = self.cfg_mgr.domains_list
        domain = domain.split(",")
        if db_name in domain:
            raise ValueError("Team name provided is Domain, please \
             provide your team name")
        else:
            self.perf_inventory.wipe_perf_env(db_name, reingest)

    def gen_kite_workflow(self, request_file):
        required_keys = ['source_database_name', 'source_table_name',
                         'hdfs_loc']
        optional_keys = []
        wf_name = self._get_workflow_name(request_file)
        requests, msg, _ = parse_file_by_sections(request_file, '[Request]',
                                                  required_keys, optional_keys)
        table_list = []
        for table in requests:
            table_list.append(table['source_table_name'])
        gen_files = []
        workflow_gen = WorkflowGenerator(wf_name, self.cfg_mgr)
        workflow_gen.gen_kite_ingest_workflow(requests)
        # Generate job.properties, kornshell script
        self.utilities.gen_kornshell(wf_name + '.xml')
        self.utilities.gen_job_properties(wf_name, table_list)
        # self.utilities.dryrun_workflow(wf_name)
        gen_files += [wf_name + '.xml', wf_name + '_job.properties',
                      wf_name + '.ksh']
        return gen_files

    def run_oozie_job(self, workflow_name):
        """Kick off an oozie job for specified workflow.
        :param workflow A workflow xml name
        :return Boolean, String A True/False and a String message
        """
        return self.utilities.run_xml_workflow(workflow_name)

    def save_it_table(self, source_type):
        """Write all of the records in the it_table to a file."""
        file_name = 'it_table_{date}_{time}'
        file_name = file_name.format(date=time.strftime("%Y%m%d"),
                                     time=time.strftime("%H%M%S"))
        return self.it_inventory.save_all_tables(file_name, source_type)

    def _set_prod_table(self):
        """For prod workflow generation, we always connect to
        ibis.prod_it_table
        """
        self.cfg_mgr.it_table = self.cfg_mgr.prod_it_table

    def gen_schedule_request(self, tables, wf_name, appl_id):
        """Create workflow for a given list of tables
        Args:
            tables: List[ibis.model.table.ItTable]
            wf_name: String value used to name xml file
            appl_id: esp id
        Returns:
            list of generated files:
                .xml, _job.properties, .ksh, _props_job.xml, .pdf
        """
        gen_files = []
        workflow_gen = WorkflowGenerator(wf_name, self.cfg_mgr)
        table_names = [tbl.table_name for tbl in tables]
        loads_map = workflow_gen.sort_table_prop_by_load(tables)
        pipelines = workflow_gen.gen_pipelines(loads_map)

        if pipelines:
            workflow_gen.gen_workflow_from_pipelines(pipelines)
            self.build_table_wf_map(tables, wf_name)
            # Generate xml workflow diagram in pdf format
            self.vizoozie.visualizeXML(wf_name)
            # Generate job.properties, kornshell script
            self.utilities.gen_kornshell(wf_name + '.xml')
            self.utilities.gen_job_properties(
                wf_name, table_names, appl_id, tables[0])
            # include custom scripts if any
            gen_files += workflow_gen.action_builder.custom_action_scripts
            # self.utilities.dryrun_workflow(wf_name)
            gen_files += [wf_name + '.xml', wf_name + '_job.properties',
                          wf_name + '.ksh', wf_name + '_props_job.xml',
                          wf_name + '.pdf']
        else:
            self.logger.error('No pipelines generated')
        return gen_files

    def gen_schedule_subworkflows(self, sub_wf_file_name, workflows_chunks,
                                  tables, appl_id):
        """Generate subworkflows
        Args:
            sub_wf_file_name: (String) File name of subworkflow
            workflows_chunks:
                List[List[ibis.model.table.ItTable, xml file name]]
            tables: List[ibis.model.table.ItTable]
            appl_id: (string) ESP appl id
        Returns:
            list of generated files:
            .xml, _job.properties, .ksh, _props_job.xml
        """
        gen_files = []
        gen = WorkflowGenerator(sub_wf_file_name, self.cfg_mgr)
        loads_map = gen.sort_table_prop_by_load(tables)
        pipelines = gen.gen_pipelines(loads_map, is_sub=True)

        if pipelines:
            self.build_table_wf_map(tables, sub_wf_file_name, False, True)
            gen.workflows_chunks = workflows_chunks
            gen.gen_workflow_from_pipelines(pipelines, is_sub=True)
            # Generate job.properties and kornshell
            # script for sub workflow
            self.utilities.gen_kornshell(sub_wf_file_name)
            req_tables_names = [table.db_table_name for table in tables]
            self.utilities.gen_job_properties(
                sub_wf_file_name, req_tables_names, appl_id)
            # self.utilities.dryrun_workflow(sub_wf_file_name)
            gen_files.extend([sub_wf_file_name + '.xml',
                              sub_wf_file_name + '_job.properties',
                              sub_wf_file_name + '_props_job.xml',
                              sub_wf_file_name + '.ksh'])
        else:
            self.logger.error('Subworkflow not generated!')

        return gen_files

    def gen_subworkflow(self, workflows_chunks, tables, subwf_name_prefix,
                        appl_id=None):
        """Generate sub workflow
        Args:
            workflows_chunks:
                List[List[List[ibis.model.table.ItTable, xml file name]]]
            tables: List[ibis.model.table.ItTable]
            subwf_name_prefix: (string)Name prefix for subworkflow
            appl_id: (string) ESP appl id
        Returns:
            gen_files: Generated subworkflows
            msg: success message
        """
        gen_files = []
        msg = ''
        # Generate subworkflow(s) to join individual workflows
        for i, workflows in enumerate(workflows_chunks):
            if appl_id:
                # prod workflow
                subworkflow_file_name = '{name}_{num}'.format(
                    name=subwf_name_prefix, num=i + 1)
            else:
                # lower environment workflow
                if i == 0:
                    subworkflow_file_name = '{name}'.format(
                        name=subwf_name_prefix)
                else:
                    subworkflow_file_name = '{name}_{num}'.format(
                        name=subwf_name_prefix, num=i)
            _files = self.gen_schedule_subworkflows(
                subworkflow_file_name, workflows, tables, appl_id)
            gen_files.extend(_files)
            _msg = 'Generated subworkflow: {path}{workflow}.xml'
            _msg = _msg.format(path=self.cfg_mgr.files,
                               workflow=subworkflow_file_name)
            self.logger.info(_msg)
            msg += _msg
        return gen_files, msg

    def _group_workflows(self, generated_workflows):
        """Group workflows into subworkflows
        Args:
            generated_workflows: List[ibis.model.table.ItTable, xml file name]
        Returns:
            workflows_chunks:
                List[List[List[ibis.model.table.ItTable, xml file name]]]
        """
        # Break up list of generated files into multiple lists
        max_tbl_per_wrkflw = self.cfg_mgr.max_table_per_workflow
        workflows_chunks = []
        for num in xrange(0, len(generated_workflows), max_tbl_per_wrkflw):
            _grouped_wf = generated_workflows[num:num + max_tbl_per_wrkflw]
            workflows_chunks.append(_grouped_wf)
        return workflows_chunks

    def gen_prod_workflow_tables(self, request_file):
        """Generate prod workflows for given tables.
           For the given tables, generate esp id if missing and then
           generate prod workflows for that esp-id
        Args:
            request_file: instance of open() request file with tables
        """
        self._set_prod_table()

        status = False
        requests, msg = self.req_inventory.parse_file(request_file)

        # update It-table before generating workflow
        self.update_it_table(requests, msg)

        if msg:
            self.logger.error(msg)
            return status

        if requests:
            available_tables, _, _ = \
                self.req_inventory.get_available_requests(requests)
            self._gen_prod_wf_tables(available_tables)
            status = True
        return status

    def _gen_prod_wf_tables(self, tables):
        """For the given tables, generate esp id if missing
        and then generate prod workflows for that esp-id
        Args:
            tables: List[ibis.model.table.ItTable]
        """
        self._set_prod_table()

        appl_ids = []
        for table in tables:
            appl_id = table.esp_appl_id
            self.logger.info('Table: {0} - Esp id: {1}'.format(
                table.full_sql_name, repr(appl_id)))
            if not appl_id:
                # generate and update appl-id
                appl_id = self.esp_inventory.gen_esp_id(
                    table.frequency_readable, table.domain, table.database,
                    table.esp_group)
                if appl_id is None:
                    warn_msg = "Scheduled workflows not generated for '{0}'!"
                    warn_msg = warn_msg.format(table.full_sql_name)
                    self.logger.warning(warn_msg)
                else:
                    table.esp_appl_id = appl_id
                    self.it_inventory.update(table)
                    msg = "Inserted new appl_id: '{0}' for '{1}'".format(
                        appl_id, table.full_sql_name)
                    self.logger.info(msg)
            if appl_id is not None and appl_id not in appl_ids:
                appl_ids.append(appl_id)
        self.logger.info('Appl ids: {0}'.format(appl_ids))
        for appl_id in appl_ids:
            status, msg, git_files = self.gen_prod_workflow(appl_id)

    def gen_prod_workflow(self, appl_id, no_git=False):
        """For a given appl_id, generate workflows for all the
           associated tables
        Args:
            appl_id: esp appl id
        Returns:
            status: (boolean) success value
            msg: status message
            git_files:
                list of generated files of workflows and subworkflows:
                .xml, _job.properties, .ksh, _props_job.xml, .wld, .pdf
        """
        self._set_prod_table()
        msg = ''
        status = False
        git_files = []
        generated_workflows = []

        try:
            tables = self.it_inventory.get_all_tables_for_esp(appl_id)
            if len(tables) == 0:
                msg += "No tables found for esp_appl_id: '{id}'\n".format(
                    id=appl_id)
                msg += "No workflow generated!\n"
                self.logger.warning(msg)
                return status, msg, git_files

            appl_refs = self.esp_inventory.get_tables_by_id(appl_id)
            if len(appl_refs) == 0:
                msg += ("No row found for esp_appl_id: '{id}' "
                        "in '{tbl}' table.\n")
                msg = msg.format(id=appl_id, tbl=self.cfg_mgr.esp_ids_table)
                msg += "No workflow generated!\n"
                self.logger.warning(msg)
                return status, msg, git_files

            self.logger.info("Generating workflow(s) for {id}".format(
                id=appl_id))
            # fetch ddl query results in parallel
            self.sqoop_cache.cache_ddl_queries(tables)
            self.sqoop_cache.cache_ddl_views(tables)
            workflow_names = []
            # Generate workflow(s)
            for table in tables:
                if self.cfg_mgr.env.lower() == 'perf' or \
                        self.cfg_mgr.env.lower() == 'dev_perf':
                    all_views = table.views_list
                    domain = self.cfg_mgr.domains_list
                    domain = domain.split(",")
                    domain = set(domain).intersection(all_views)
                    domain = list(domain)
                    freq = ItTable.frequency_readable_helper(table.frequency)
                    full_tb_nm = table.full_name
                    if len(all_views) != len(domain) and len(domain) > 0\
                            and domain[0]:
                        for view_nm in all_views:
                            if domain[0] == view_nm.lower():
                                continue
                            self.perf_inventory.insert_freq_ingest([view_nm],
                                                                  [freq],
                                                                  [full_tb_nm],
                                                                  ['default'])
                    elif len(all_views) != len(domain):
                        for view_nm in all_views:
                            self.perf_inventory.insert_freq_ingest([view_nm],
                                                                  [freq],
                                                                  [full_tb_nm],
                                                                  ['default'])

                wf_name = self._get_prod_table_workflow_name(table)
                workflow_names.append(wf_name)
                # Generate workflow for each table
                gen_files = self.gen_schedule_request([table], wf_name,
                                                      appl_id)
                git_files += gen_files
                if self.cfg_mgr.env.lower() == 'perf' or \
                        self.cfg_mgr.env.lower() == 'dev_perf'and\
                        len(all_views) != len(domain):
                    hql_name = self.perf_inventory.save_perf_hql(table)
                    git_files.extend(hql_name)
                generated_workflows.append([table, wf_name])
                _msg = 'Generated workflow {path}{file_name}.xml\n'
                _msg = _msg.format(path=self.cfg_mgr.files,
                                   file_name=wf_name)
                self.logger.info(_msg)
                msg += _msg

            incr_files = self.gen_incr_workflow_files(tables)
            if incr_files:
                git_files += incr_files

            workflows_chunks = self._group_workflows(generated_workflows)
            gen_files, _msg = self.gen_subworkflow(
                workflows_chunks, tables, appl_refs[0]['ksh_name'], appl_id)
            git_files.extend(gen_files)
            if not self.dryrun_parallel.run_all(git_files):
                err_msg = 'Dryrun failed. Fix the workflow!'
                return status, err_msg, git_files
            msg += _msg
            # Generate wld file
            wld_file = self.esp_inventory.gen_wld_tables(
                appl_id, tables, workflow_names)
            git_files.append(wld_file)
            self.utilities.chmod_files(git_files)
            status = True
        except Exception:
            err_msg = "Error generating workflow for " \
                      "esp_appl_id: '{id}'\n".format(id=appl_id)
            err_msg += traceback.format_exc()
            err_msg = 'Error found in driver.gen_prod_workflow - ' \
                      'reason: \n{0}'.format(err_msg)
            self.logger.error(err_msg)
            msg = err_msg
        return status, msg, git_files

    def generate_subworkflow(self, workflow_name, file_names):
        """Generate subworkflows with file.
        :param workflow_name String
        :param file_names List[String]
        :return msg string
        """
        gen = WorkflowGenerator(workflow_name, self.cfg_mgr)
        gen.generate_subworkflow(file_names)
        self.utilities.gen_kornshell(workflow_name)
        self.utilities.gen_job_properties(workflow_name,
                                          ['Check subworkflows'])
        msg = 'Generated subworkflow: {path}{workflow}.xml \n'
        msg = msg.format(path=self.cfg_mgr.files, workflow=workflow_name)
        self.logger.info(msg)
        return msg

    def export(self, db, table, to):
        """Used to export hadoop table to teradata.
        :param db database name in hadoop
        :param table table name in hadoop
        :param to database.table name in teradata to export to
        :return msg String
        """
        msg = 'Exporting to Teradata\n'
        export = {}
        # Check if table exists in it_table to get directory to export
        tbl = self.it_inventory.get_table_mapping(db, table)
        try:
            to = to.split('.')
            export['db'] = to[0]
            export['table'] = to[1]
            if tbl:
                export['action_name'] = table + '_export'
                export_dir = '/user/data/{tar_dir}'.\
                    format(tar_dir=tbl['target_dir'])
                export['export_dir'] = export_dir
                export['ok'] = 'end'
                wrkflw_gen = WorkflowGenerator(export['action_name'],
                                               self.cfg_mgr)
                wrkflw_gen.generate_td_export(export)

                # Generate job.properties and kornshell
                self.utilities.gen_kornshell(export['action_name'])
                self.utilities.gen_job_properties(export['action_name'],
                                                  [table])
                self.utilities.dryrun_workflow(export['action_name'])
                msg += 'Workflow, {workflow}.xml, generated to {loc}.'
                msg = msg.format(workflow=export['action_name'],
                                 loc=self.cfg_mgr.files)
                self.logger.info(msg)
            else:
                msg += ('{db} {table} doesn\'t exist in the it_table export'
                        ' directory could not be found.\n')
                msg = msg.format(db=db, table=table)
                self.logger.error(msg)
        except IndexError as err:
            msg += 'Please provide an appropriate --to {db}.{table}'
            self.logger.error('Error found in driver.export - '
                              'reason: %s ' % err.message)
        return msg

    def export_database(self, tables):
        """Used to export hadoop table to oracle.
        :param source_database_name database name in hadoop
        :param source_table_name table name in hadoop
        :param target_database_name schema name in oracle to export to
        :param target_table_name table name in oracle
        :return msg String
        """
        msg = 'Exporting to RDBMS\n'
        table = tables[0]
#         table = tables[0].db_table_name
        workflow_name = self._get_workflow_name_table_export(table)

        # TODO - need to check out what changes make sense in the IT table
        # to support export to RDBMS -
        # TODO - or whether to just leave it as all cmd line arguments
        # since it won't be frequently used
        # Check if table exists in it_table to get directory to export
        # tbl = self.it_inventory.get_table_mapping(target_database_name,
        # target_table_name)
        workflow_gen = WorkflowGenerator(workflow_name, self.cfg_mgr)
        workflow_gen.gen_database_export(tables)
        workflow_gen.file_out.close()

        # Generate job.properties and kornshell
        self.utilities.gen_kornshell(workflow_name)
        self.utilities.gen_job_properties(workflow_name,
                                          [tables[0].table_name])
        self.utilities.dryrun_workflow(workflow_name)
        msg += 'Workflow {workflow}.xml generated to {loc}.'
        msg = msg.format(workflow=workflow_name, loc=self.cfg_mgr.files)
        self.logger.info(msg)

    def export_oracle(self, source_table_name, source_database_name,
                      source_dir, jdbc_url,
                      update_key, target_table_name, target_database_name,
                      user_name,
                      password_alias):
        """Used to export hadoop table to oracle.
        :param source_database_name database name in hadoop
        :param source_table_name table name in hadoop
        :param target_database_name schema name in oracle to export to
        :param target_table_name table name in oracle
        :return msg String
        """
        msg = 'Exporting to Oracle\n'
        workflow_name = source_table_name + "_export"

        # TODO - need to check out what changes make sense in the IT table
        # to support export to Oracle -
        # TODO - or whether to just leave it as all cmd line arguments
        # since it won't be frequently used
        # Check if table exists in it_table to get directory to export
        # tbl = self.it_inventory.get_table_mapping(target_database_name,
        # target_table_name)
        workflow_gen = WorkflowGenerator(workflow_name, self.cfg_mgr)
        workflow_gen.gen_oracle_export(source_table_name, source_database_name,
                                       source_dir,
                                       jdbc_url, update_key, target_table_name,
                                       target_database_name, user_name,
                                       password_alias)
        workflow_gen.file_out.close()

        # Generate job.properties and kornshell
        self.utilities.gen_kornshell(workflow_name)
        self.utilities.gen_job_properties(workflow_name, [source_table_name])
        self.utilities.dryrun_workflow(workflow_name)
        msg += 'Workflow {workflow}.xml generated to {loc}.'
        msg = msg.format(workflow=workflow_name, loc=self.cfg_mgr.files)
        self.logger.info(msg)

    def export_teradata(self, source_table_name, source_database_name,
                        source_dir, jdbc_url, target_table_name,
                        target_database_name, user_name, password_alias):
        """Used to export hadoop table to oracle.
        :param source_database_name database name in hadoop
        :param source_table_name table name in hadoop
        :param target_database_name schema name in oracle to export to
        :param target_table_name table name in oracle
        :return msg String
        """
        msg = 'Exporting to Teradata\n'
        workflow_name = source_table_name + "_export"

        # TODO - need to check out what changes make sense in the IT table
        # to support export to Oracle -
        # TODO - or whether to just leave it as all cmd line arguments
        # since it won't be frequently used
        # Check if table exists in it_table to get directory to export
        # tbl = self.it_inventory.get_table_mapping(target_database_name,
        # target_table_name)
        workflow_gen = WorkflowGenerator(workflow_name, self.cfg_mgr)

        workflow_gen.gen_teradata_export(
            source_table_name, source_database_name, source_dir, jdbc_url,
            target_table_name, target_database_name, user_name,
            password_alias)
        workflow_gen.file_out.close()

        # Generate job.properties and kornshell
        self.utilities.gen_kornshell(workflow_name)
        self.utilities.gen_job_properties(workflow_name, [source_table_name])
        self.utilities.dryrun_workflow(workflow_name)
        msg += 'Workflow {workflow}.xml generated to {loc}.'
        msg = msg.format(workflow=workflow_name, loc=self.cfg_mgr.files)
        self.logger.info(msg)

    def gen_it_table_with_split_by(self, tables_fh, timeout):
        """Given a list of tables, and an IT table request skeleton.
        finds the best split by for tables
        """
        auto_fh = create(tables_fh, self.cfg_mgr, timeout)
        if auto_fh:
            self.submit_it_file(auto_fh)

    def auth_test(self, jdbc_url, source_db, source_table, user_name,
                  password_file):
        """Test sqoop auth"""
        print jdbc_url, source_db, source_table, user_name, password_file
        auth_obj = AuthTest(self.cfg_mgr, source_db, source_table, jdbc_url)
        status = auth_obj.verify(user_name, password_file)
        if not status:
            msg = ('Authentication check failed.\n'
                   'Contact DBA to grant SELECT access to the table.')
            box_msg = Utilities.print_box_msg(msg, border_char='x')
            raise ValueError(box_msg)

    def parse_request_file(self, request_file):
        """Print tables in request file as json. Useful for web app
        Args:
            request_file: instance of open()
        """
        requests, msg = self.req_inventory.parse_file(request_file)
        if msg:
            return msg
        for table_req in requests:
            self.logger.info('json:' + json.dumps(table_req.get_meta_data()))

    def retrieve_backup(self, db_name, table_name):
        """Retrieve the backup in /user/data/backup and place in live."""
        export_type = False
        tbl = self.it_inventory.get_table_mapping(db_name, table_name,
                                                  export_type)
        msg = 'Retrieving backup for {db}_{tbl}\n'.format(db=db_name,
                                                          tbl=table_name)
        if tbl:
            tar_dir = tbl.get('target_dir')
            if tar_dir:
                backup_dir = '/user/data/backup/{tar_dir}'.\
                    format(tar_dir=tar_dir)
                try:
                    # Check if path exists
                    if self.utilities.run_subprocess(['hadoop', 'fs',
                                                      '-test', '-e',
                                                      backup_dir]) == 0:
                        # Check if gen and live directory exists
                        gen_dir = '/user/data/{tar_dir}/gen/'.\
                            format(tar_dir=tar_dir)
                        live_dir = '/user/data/{tar_dir}/live/'.\
                            format(tar_dir=tar_dir)
                        if self.utilities.run_subprocess(
                                ['hadoop', 'fs', '-test', '-e', gen_dir]) != 0:
                            subprocess.call(['hadoop', 'fs', '-mkdir',
                                             '-p', gen_dir])
                        if self.utilities.run_subprocess(
                                ['hadoop', 'fs', '-test', '-e', live_dir]) \
                                != 0:
                            subprocess.call(['hadoop', 'fs', '-mkdir',
                                             '-p', live_dir])

                        # Copy files
                        if self.utilities.\
                                run_subprocess(['hadoop',
                                                'fs', '-cp', '-f',
                                                backup_dir +
                                                '/gen/parquet_live.hql',
                                                gen_dir]) != 0:
                            msg += 'Failed to copy parquet_live.hql\n'
                        if self.utilities.\
                                run_subprocess(['hadoop', 'fs', '-cp', '-f',
                                                backup_dir +
                                                '/gen/avro_parquet.hql',
                                                gen_dir]) != 0:
                            msg += 'Failed to copy avro_parquet.hql\n'

                        if self.utilities.run_subprocess(['hadoop', 'fs',
                                                          '-cp', '-f',
                                                          backup_dir + '/*',
                                                          live_dir]) != 0:
                            msg += 'Failed to copy files to live.\n'
                        else:  # Clean up
                            self.utilities.run_subprocess(['hadoop', 'fs',
                                                           '-rm', '-r',
                                                           live_dir + '_gen'])
                            self.utilities.run_subprocess(['hadoop', 'fs',
                                                           '-rm', '-r',
                                                           live_dir + 'gen'])
                    else:
                        msg += 'Backup doesn\'t exist.\n'
                except subprocess.CalledProcessError as e:
                    self.logger.error('Error found in driver.retrieve'
                                      '_backup, exit '
                                      'from process with errors. '
                                      'Subprocess error: %s' % e.message)
                    raise ValueError('Failed')
            else:
                msg += 'Target directory doesn\'t exist in it_table.\n'
        else:
            msg += '{db}_{tbl} does not exist in it_table. Directory' \
                   ' can\'t be found\n'.format(db=db_name, tbl=tbl)
        return msg

    def update_lifespan(self, db_name, tbl_name, lifespan):
        """Update the lifespan in Check and Balances table.
        :param db_name Name of the db
        :param tbl_name Name of the table
        :param lifespan String value
        :return String message
        """
        tbl = self.cb_inventory.get(db_name, tbl_name)
        if tbl:
            tbl_meta = {}
            tbl = tbl[0]
            tbl_meta['directory'] = tbl[0]
            tbl_meta['pull_time'] = tbl[1]
            tbl_meta['avro_size'] = tbl[2]
            tbl_meta['ingest_timestamp'] = tbl[3]
            tbl_meta['parquet_time'] = tbl[4]
            tbl_meta['parquet_size'] = tbl[5]
            tbl_meta['rows'] = tbl[6]
            tbl_meta['lifespan'] = lifespan
            tbl_meta['ack'] = tbl[8]
            tbl_meta['cleaned'] = tbl[9]
            tbl_meta['current_repull'] = tbl[10]
            tbl_meta['domain'] = tbl[11]
            tbl_meta['table'] = tbl[12]
            self.cb_inventory.update(tbl_meta)
            msg = 'Record for {tbl} updated with new lifespan in' \
                  ' checks_balances'
            msg = msg.format(tbl=tbl_meta['table'])
            return msg
        else:
            msg = 'Record doesn\'t exist for table={db}_{tbl} in ' \
                  'checks_balances'
            msg = msg.format(db=db_name, tbl=tbl_name)
            return msg

    def update_all_lifespan(self):
        """Based on it table load values, updates all lifespan in
        Checks and Balances."""
        all_tables = self.it_inventory.get_all_tables()
        msg = ''
        for table in all_tables:
            try:
                frequency = table['load'][0:3]
                if frequency in self.cfg_mgr.allowed_frequencies.keys():
                    msg += '\n' + self.update_lifespan(
                        table['source_database_name'],
                        table['source_table_name'],
                        self.cfg_mgr.allowed_frequencies[frequency])
            except IndexError as ie:
                msg += '\n' + 'Table, {table} had invalid ' \
                              'load value'.\
                    format(table=table['full_table_name'])
                self.logger.error('Error found in driver.update_'
                                  'all_lifespan -reason %s ' % ie.message)
        return msg

    def update_it_table_export(self, request_tables, msg):
        """Updates or inserts new it table export row"""
        if not request_tables:
            return msg

        tables = self.export_it_inventory.parse_requests_export(request_tables)

        for modified_table_obj in tables:
            table_dict = self.export_it_inventory.get_table_mapping(
                modified_table_obj.database, modified_table_obj.table_name,
                modified_table_obj.db_env)
            existing_table_obj = \
                ItTableExport(table_dict, self.cfg_mgr) if table_dict else {}
            modified_columns, modified_values = \
                self.get_export_table_diff(modified_table_obj,
                                           existing_table_obj)

            if modified_columns:
                self.logger.info('Modified columns: ' +
                                 ', '.join(modified_columns))
                self.logger.info('Modified values: ' +
                                 ', '.join(modified_values))
            if not existing_table_obj:
                # insert
                success, msgg = self.export_it_inventory.insert_export(
                    modified_table_obj)
                msg += '\n' + msgg
            else:
                # update
                if modified_columns:
                    success, msgg = self.export_it_inventory.update_export(
                        modified_table_obj)
                    if success:
                        self.logger.info('Updated: {0}'.format(
                            modified_table_obj.db_table_name))
                        msg += '\n' + msgg
                else:
                    self.logger.warning('Nothing to update: {0}'.format(
                        modified_table_obj.db_table_name))
        return msg

    def get_export_table_diff(self, modified_table_obj, existing_table_obj):
        """Return the diff between new table and existing table in
        it_table in column-values
        Args:
            modified_table_obj: instance of ibis.model.table.ItTableExport
            existing_table_obj: instance of ibis.model.table.ItTableExport
        """
        modified_table = modified_table_obj.get_meta_dict()
        modified_columns = []
        modified_values = []
        for column, modified_val in modified_table.iteritems():
            old_val = ''
            if existing_table_obj and column not in REQUIRED_FIELDS_EXPORT:
                if column == 'source_schema_name':
                    old_val = existing_table_obj.schema
                elif column == 'db_username':
                    old_val = existing_table_obj.username
                elif column in OPTIONAL_FIELDS_EXPORT:
                    old_val = getattr(existing_table_obj, column)

            if column in REQUIRED_FIELDS_EXPORT:
                continue

            if modified_val == 'null':
                if column == 'source_schema_name':
                    modified_table_obj.schema = ''
                elif column == 'weight':
                    modified_table_obj.load_readable = ''
                elif column == 'refresh_frequency':
                    modified_table_obj.frequency_readable = ''
                elif column == 'db_username':
                    modified_table_obj.username = ''
                else:
                    setattr(modified_table_obj, column, '')
                modified_columns.append(column)
                modified_values.append('<empty>')
            elif modified_val == '':
                # key: in request file is ignored
                pass
            elif str(modified_val) != str(old_val) and modified_val != '':
                if column == 'source_schema_name':
                    modified_table_obj.schema = modified_val
                elif column == 'weight':
                    modified_table_obj.load_readable = modified_val
                elif column == 'refresh_frequency':
                    modified_table_obj.frequency_readable = modified_val
                modified_columns.append(column)
                modified_values.append(str(modified_val))
        return modified_columns, modified_values

    def export_request(self, request_file, no_git):
        """Used to submit a file containing a request table.
        Queries the it table export to see which table(s) are available for
        workflow generation.
        Places a placeholder record for tables requiring access to.
        :param request_file A text file contains sections of [Request]
        :return msg A string
        """
        status = False
        requests, msg = self.req_inventory.parse_file_export(request_file)
        # update It-table-export before generating workflow
        msg = self.update_it_table_export(requests, msg)
        tables, _ = \
            self.req_inventory.get_available_requests_export(requests)

        self.export_database(tables)
        status = True
        return status, msg

    def gen_config_workflow(self, config_file, config_workflow_properties,
                            queue_name):
        """Generate config based workflows
        Args:
            config_file: instance of open() which contains actions config
            config_workflow_properties: properties file name
            queue_name: str, hadoop queue name
        """
        if config_workflow_properties:
            wf_props = self.cfg_mgr.read_config_wf_props(
                config_workflow_properties)
            # override the values
            self.cfg_mgr.custom_scripts_hql = wf_props.get(
                'Workflows', 'custom_scripts_hql')
            self.cfg_mgr.custom_scripts_shell = wf_props.get(
                'Workflows', 'custom_scripts_shell')
            self.cfg_mgr.oozie_workspace = wf_props.get(
                'Workflows', 'workspace')
            self.cfg_mgr.saves = wf_props.get('Workflows', 'deploy_path')

        if queue_name:
            # setup custom queue name
            self.cfg_mgr.queue_name = queue_name
            self.logger.info("Queue name set to: '{0}'".format(queue_name))

        scripts_dir = os.path.abspath(config_file.name).rsplit('/', 1)[-2]
        msg = 'Assuming files are in directory: {0}'
        msg = msg.format(scripts_dir)
        self.logger.info(msg)
        config_file_name = os.path.abspath(config_file.name).rsplit('/', 1)[-1]
        wf_name = self._get_config_workflow_name(config_file)
        wf_gen = WorkflowGenerator(wf_name, self.cfg_mgr)
        wf_gen.gen_config_workflow(config_file_name, scripts_dir)
        # Generate job.properties, kornshell script
        self.utilities.gen_kornshell(wf_name + '.xml')
        self.utilities.gen_job_properties(wf_name, [], None)
        gen_files = [wf_name + '.xml', wf_name + '_job.properties',
                     wf_name + '.ksh', wf_name + '_props_job.xml']
        # include custom scripts
        gen_files += wf_gen.action_builder.custom_action_scripts
