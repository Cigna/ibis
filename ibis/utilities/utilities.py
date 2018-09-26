"""Utility methods used for ibis."""
import os
import string
import subprocess
import itertools
import getpass
import re
import requests
from mako.template import Template
from ibis.custom_logging import get_logger
from ibis.utilities.oozie_helper import OozieAPi

try:
    # available only on linux
    from pwd import getpwuid
except ImportError:
    pass

requests.packages.urllib3.disable_warnings()


class Utilities(object):
    """Provides additional functionality to ibis"""

    def __init__(self, cfg_mgr):
        self.cfg_mgr = cfg_mgr
        self.logger = get_logger(self.cfg_mgr)
        self.oozie_api = OozieAPi(self.cfg_mgr)

    def get_lines_from_file(self, file_name):
        """Returns lines from a file"""
        lines = ''
        try:
            with open(file_name, "r") as file_h:
                lines = file_h.readlines()
        except IOError as ie:
            err_msg = 'Cannot open {file} .'.format(file=file_name)
            self.logger.error(err_msg)
        return lines

    def gen_job_properties(self, workflow_name, table_list, appl_id=None,
                           table=None):
        """Generates the _job.properties file
        Args:
            workflow_name: just the workflow name without extension
            table_list: list of table names
            appl_id: esp appl id
            table: it_table row
        """
        job_prop = self.get_lines_from_file(self.cfg_mgr.job_prop_template)
        status = True
        job_properties = []
        if not appl_id:
            appl_id = None
        name_val = workflow_name.split('.')[0]
        file_name = os.path.join(self.cfg_mgr.files,
                                 '{0}_job.properties'.format(name_val))

        for prop_val in job_prop:
            job_properties.append(prop_val)

        wf_path = 'oozie.wf.application.path=${nameNode}' + self. \
            cfg_mgr.oozie_workspace + workflow_name + '.xml\n'
        wf_path = string.Formatter().vformat(wf_path, (), SafeDict(
            {'workflow_name': workflow_name}))
        job_properties.append(wf_path)

        if table:
            _lines = []
            _lines.append('source_table_name={0}\n'.format(table.table_name))
            _lines.append('source_database_name={0}\n'.format(table.database))
            if table.query:
                _lines.append('sql_query={0}\n'.format(table.query))
            job_properties += _lines

        with open(file_name, "wb+") as file_h:
            line = ''.join(job_properties)
            file_h.write(line)
            file_h.write('workflowName={job}\n'.format(job=workflow_name))
            if table_list:
                _line = '#List of tables ingested: {0}\n'
                _line = _line.format(', '.join(table_list))
                file_h.write(_line)

        self.logger.info('Generated job properties file: {0}'.format(
            file_name))
        self.gen_job_config_xml(name_val, job_properties)
        return status

    def gen_job_config_xml(self, workflow_name, job_properties):
        """Generates job config xml for starting jobs via oozie api"""
        wf_props = {}
        for line in self.clean_lines(job_properties):
            prop_name, prop_val = line.split('=')
            wf_props[prop_name] = prop_val
        # TODO: Move username to be a property
        wf_props["user.name"] = "fake_username"
        template = Template(filename=self.cfg_mgr.job_config_xml_template,
                            format_exceptions=True)
        xml = template.render(wf_props=wf_props)
        file_name = os.path.join(self.cfg_mgr.files,
                                 '{0}_props_job.xml'.format(workflow_name))
        with open(file_name, "wb+") as file_h:
            file_h.write(xml)
            self.logger.info('Generated job config xml: {0}'.format(file_name))

    def run_workflow(self, workflow_name):
        """Runs oozie properties file on host
        Args:
            workflow_name: name of the workflow without extension
        """
        run = False
        config_file = os.path.join(self.cfg_mgr.saves, '{name}_job.properties')
        config_file = config_file.format(name=workflow_name)
        xml_file = os.path.join(self.cfg_mgr.oozie_workspace,
                                '{name}.xml'.format(name=workflow_name))
        path_exists_cmd = ['hadoop', 'fs', '-test', '-e', xml_file]
        self.logger.info(" ".join(path_exists_cmd))
        path_exists_ret = self.run_subprocess(path_exists_cmd)
        if path_exists_ret != 0:
            self.logger.error('XML file missing in HDFS: {0}'.format(xml_file))
            return run

        command = ['oozie', 'job', '-config', config_file, '-run']
        self.logger.info('Running: ' + ' '.join(command))
        proc = subprocess.Popen(command, stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        output, err = proc.communicate()
        if proc.returncode == 0:
            self.logger.info(output)
            msg = 'Workflow {0} started - please check Hue'.format(
                workflow_name)
            self.logger.info(msg)
            run = True
        else:
            self.logger.error("Workflow {0} did not run".format(workflow_name))
            self.logger.error(err)
        return run

    def run_xml_workflow(self, workflow_name):
        """Runs oozie properties xml file on host
        Args:
            workflow_name: name of the workflow without extension
        """
        run = False
        config_file = os.path.join(self.cfg_mgr.saves, '{name}_props_job.xml')
        config_file = config_file.format(name=workflow_name)

        if not self.cfg_mgr.for_env:
            env = self.cfg_mgr.env
        else:
            env = self.cfg_mgr.for_env

        if 'dev' in env:
            os.environ['KRB5_CONFIG'] = "/opt/app/kerberos/dev_krb5.conf"
            keytab = 'fake.keytab'
        elif 'int' in env:
            os.environ['KRB5_CONFIG'] = "/opt/app/kerberos/int_krb5.conf"
            keytab = 'fake.keytab'
        elif 'prod' in env:
            os.environ['KRB5_CONFIG'] = "/opt/app/kerberos/prod_krb5.conf"
            keytab = 'fake.keytab'
        else:
            raise ValueError('Unrecognized --for-env value: {0}'.format(
                self.cfg_mgr.for_env))

        command = ["kinit", "-S",
                   "krbtgt/{0}.COM@{0}.COM".format(self.cfg_mgr.kerberos),
                   "fake_username@{0}.COM".format(self.cfg_mgr.kerberos),
                   "-k", "-t", "/opt/app/kerberos/{0}".format(keytab)]
        proc = subprocess.Popen(command, stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        output, err = proc.communicate()
        if proc.returncode == 0:
            self.logger.info(output)
            msg = 'kinit successful'
            self.logger.info(msg)
            run = self.oozie_api.start_job(config_file)
            if not run:
                self.logger.error("Job didn't run!")
            else:
                self.logger.info("Job started. Please check HUE")
        else:
            self.logger.error('kinit failed')
            self.logger.error(output)
            self.logger.error(err)
        return run

    def put_dry_workflow(self, workflow_name):
        """Put workflow xml to hdfs
        Args:
            workflow_name: just the workflow name without extension
        """
        temp_folder = 'tmp'
        temp = '/' + temp_folder + '/'
        workflow_full_path = os.path.join(self.cfg_mgr.files,
                                          workflow_name + '.xml')
        workflow_hdfs_path = os.path.join(temp, workflow_name + '.xml')
        put_cmd = ['hadoop', 'fs', '-put', '-f', workflow_full_path, temp]
        self.logger.info('Running: {0}'.format(" ".join(put_cmd)))
        put_status = self.run_subprocess(put_cmd)
        chmod_status = 1
        if put_status == 0:
            chmod_status = self.run_subprocess(
                ['hadoop', 'fs', '-chmod', '-R', '777',
                 workflow_hdfs_path])
        status = (put_status == 0 and chmod_status == 0)
        if status:
            self.logger.info(
                'Put workflow to hdfs: {0}'.format(workflow_hdfs_path))
        return status

    def gen_dryrun_workflow(self, workflow_name):
        """Generate temp workflow for dry run
        Args:
            workflow_name: just the workflow name without extension
        """
        status = False
        temp_folder = 'tmp'
        temp = '/' + temp_folder + '/'
        new_wf_name = workflow_name + '_dryrun'
        props_path = os.path.join(self.cfg_mgr.files,
                                  '{0}_job.properties'.format(workflow_name))
        with open(props_path, 'r') as props_fh:
            lines = props_fh.readlines()
            new_lines = []
            for line in lines:
                wf_file = self.cfg_mgr.oozie_workspace + workflow_name + '.xml'
                if wf_file in line:
                    new_line = line.replace(wf_file,
                                            temp + new_wf_name + '.xml')
                    new_lines.append(new_line)
                else:
                    new_lines.append(line)
            dryrun_props_file_name = '{0}_dryrun_job.properties'.format(
                workflow_name)
            dryrun_props_path = os.path.join(self.cfg_mgr.files,
                                             dryrun_props_file_name)
            with open(dryrun_props_path, 'wb') as dry_props_fh:
                dry_props_fh.write("".join(new_lines))
                msg = "Created temp properties for dryrun: {0}".format(
                    dryrun_props_path)
                self.logger.info(msg)
            self.chmod_files([dryrun_props_file_name])

            wf_path = os.path.join(self.cfg_mgr.files, workflow_name + '.xml')
            with open(wf_path, 'r') as wf_fh:
                wf_txt = wf_fh.read()
                new_wf_path = os.path.join(self.cfg_mgr.files,
                                           new_wf_name + '.xml')
                with open(new_wf_path, 'wb') as new_wf_fh:
                    new_wf_fh.write(wf_txt)
                    msg = "Created temp workflow for dryrun: {0}".format(
                        new_wf_path)
                    self.logger.info(msg)
                self.chmod_files([new_wf_name + '.xml'])
                status = self.put_dry_workflow(new_wf_name)
        return status, new_wf_name

    def dryrun_workflow(self, workflow_name):
        """Runs oozie job on host
        Args:
            workflow_name: just the workflow name without extension
        """
        status = False
        try:
            ret, new_wf_name = self.gen_dryrun_workflow(workflow_name)
            if ret:
                workflow_name = new_wf_name
                msg = 'Dry running workflow: {0}.xml'.format(workflow_name)
                self.logger.info(msg)
                config_file = os.path.join(self.cfg_mgr.files,
                                           '{0}_job.properties')
                config_file = config_file.format(workflow_name)
                cmd = ['oozie', 'job', '-config', config_file, '-dryrun']
                self.logger.info('Running: {0}'.format(" ".join(cmd)))
                proc = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE)
                _, err = proc.communicate()
                self.rm_dry_workflow(new_wf_name)
                if proc.returncode != 0:
                    self.logger.error('Dry run failed!')
                    raise ValueError(err)
                self.logger.info('Dry run successful')
                status = True
        except ValueError as error:
            err_msg = error.message
            err_msg += 'Workflow {workflow_xml}.xml is not valid'.format(
                workflow_xml=workflow_name)
            self.logger.error(err_msg)
        return status

    def rm_dry_workflow(self, workflow_name):
        """Remove the temporary workflow used for dry run"""
        temp_folder = 'tmp'
        temp = '/' + temp_folder + '/'
        file_path = temp + workflow_name + '.xml'
        rm_cmd = ['hadoop', 'fs', '-rm', file_path]
        proc = subprocess.Popen(rm_cmd, stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        _, err = proc.communicate()
        if proc.returncode != 0:
            self.logger.warning('hdfs rm {0} failed!'.format(file_path))
        else:
            self.logger.info('success: hdfs rm {0}'.format(file_path))

    def gen_kornshell(self, workflow_name):
        """Creates kornshell script for jobs scheduled with esp
        Args:
            workflow_name: just the workflow name without extension
        """
        status = False
        job_name = workflow_name.split('.')[0]
        oozie_url = self.cfg_mgr.oozie_url
        oozie_url = oozie_url.replace('/v2/', '')
        output_file = os.path.join(self.cfg_mgr.files,
                                   '{name}.ksh'.format(name=job_name))
        with open(output_file, "wb") as file_out:
            template = Template(filename=self.cfg_mgr.korn_shell_template,
                                format_exceptions=True)
            ksh_text = template.render(job_name=job_name,
                                       saved_loc=self.cfg_mgr.saves,
                                       oozie_url=oozie_url,
                                       kerberos_realm=self.cfg_mgr.kerberos)
            file_out.write(ksh_text)
            self.logger.info(
                'Generated kornshell script: {0}'.format(output_file))
            status = True
        return status

    @classmethod
    def print_box_msg(cls, msg, border_char='#'):
        """Returns a visually appealing message
        Args:
            msg: str, log string
        """
        content = ''
        max_line_len = 80
        tab_replace_len = 3
        four_spaces = ' ' * 4
        msg_lst_orig = msg.splitlines()
        msg_lst = []

        for line in msg_lst_orig:
            if len(line) > max_line_len:
                lengthy_lines = line.split(' ')
                if len(lengthy_lines) == 1:
                    msg_lst.append(line)
                else:
                    limited_lines = []
                    line_len = 0
                    new_line = ''
                    for each in lengthy_lines:
                        if line_len < max_line_len:
                            new_line += each + ' '
                            line_len += len(each)
                        else:
                            if len(limited_lines) == 0:
                                limited_lines.append(new_line)
                            else:
                                limited_lines.append('\t' + new_line)
                            new_line = each + ' '
                            line_len = len(each)
                    if len(new_line) != 0 and len(new_line) < max_line_len:
                        limited_lines.append('\t' + new_line)
                    else:
                        limited_lines.append(new_line)
                    msg_lst = msg_lst + limited_lines
            else:
                msg_lst.append(line)
        long_str_len = \
            len(max(msg_lst, key=len).replace('\t', ' ' * tab_replace_len))
        start = four_spaces + border_char * (long_str_len + 10) + '\n'
        empty_line = four_spaces + border_char + four_spaces + \
            ' ' * long_str_len + four_spaces + border_char + '\n'
        for line in msg_lst:
            line = line.replace('\t', ' ' * tab_replace_len)
            content += four_spaces + border_char + four_spaces + line + \
                ' ' * (long_str_len - len(line)) + four_spaces + \
                border_char + '\n'
        return '\n\n' + start + empty_line + content + empty_line + start

    @classmethod
    def pairwise(cls, iterable):
        """For-loop when we need current and next element in iterable"""
        first, second = itertools.tee(iterable)
        next(second, None)
        return itertools.izip(first, second)

    def run_subprocess(self, command_list):
        """Launch subprocess get return code
        :param command_list: List[String] List of commands to run
        :return Int return code"""
        process = subprocess.Popen(command_list)
        process.wait()
        return process.returncode

    def chmod_files(self, files):
        """Make files accessible from different users
        Args:
            files(list): list of file names.
        """
        file_permission = 0774
        for file_name in files:
            _path = os.path.join(self.cfg_mgr.files, file_name)
            if getpass.getuser() in Utilities.find_owner(_path):
                # self.logger.info('Chmod 777 file: {0}'.format(_path))
                os.chmod(_path, file_permission)

    def sort_tables_by_source(self, tables):
        """Given a list of tables sort by source of db
        :param tables: List[Dictionary{table}] A list of dictionary of table
        :return sources: Dictionary{List[{table}]} A dictionary of list
        of tables in a common source """
        sources = {'teradata': [],
                   'oracle': [],
                   'sqlserver': [],
                   'db2': [],
                   'mysql': []}
        for table in tables:
            for i, key in enumerate(sources.keys()):
                jdbc_key = 'jdbc:' + key
                if jdbc_key in table['jdbcurl'].lower():
                    src_tables = sources[key]
                    src_tables.append(table)
                    sources[key] = src_tables
                    break
                elif i == len(
                        sources.keys()) - 1:  # Looped through keys, no match
                    self.logger.error(
                        'Sort by source can\'t group {table} with url, '
                        '{jdbc}. Supported groupings, '
                        '{keys}'.format(table=table['source_table_name'],
                                        jdbc=table['jdbcurl'],
                                        keys=sources.keys()))
                    sources = {}
        return sources

    def sort_tables_by_domain(self, tables):
        """Given a list of tables sort by domain
        :param tables: List[Dictionary{table}] A list of dictionary of table
        :return sources: Dictionary{List[{table}]} A dictionary of list
        of tables in a common domain """
        domains = {}
        for table in tables:
            domain = table['domain']
            if domain not in domains.keys():
                domains[domain] = [table]
            else:
                dom_tables = domains[domain]
                dom_tables.append(table)
                domains[domain] = dom_tables
        return domains

    def sort_tables_by_database(self, tables):
        """Given a list of tables sort by database
        :param tables List[{table}] A list of dictionary of table
        :return databases {database: List[{table}] A dictionary of list
        of tables in a common database"""
        databases = {}
        for table in tables:
            database = table['source_database_name']
            if database not in databases.keys():
                databases[database] = [table]
            else:
                db_tables = databases[database]
                db_tables.append(table)
                databases[database] = db_tables
        return databases

    def sort_tables_by_schedule(self, tables):
        """Give a list of tables sort by schedule
        :param tables List[{table}] A list of dictionary of table
        :return databases {database: List[{table}] A dictionary of list
        of tables in a common database"""
        schedules = {}

        for schedule in self.cfg_mgr.allowed_frequencies.keys():
            schedules[schedule] = []

        for table in tables:
            try:
                frequency = table['load'][0:3]
                if frequency in schedules.keys():
                    sch_tables = schedules[frequency]
                    sch_tables.append(table)
                    schedules[frequency] = sch_tables
            except (KeyError, IndexError) as e:
                err_msg = ("Sorting tables by schedule, table {table} "
                           "has improper load value. Supported values "
                           "are {values} \n {e}")
                err_msg = err_msg.format(
                    table=table['source_table_name'],
                    values=self.cfg_mgr.allowed_frequencies.keys(), e=e)
                self.logger.error(err_msg)
                self.logger.error(
                    "Error found in utilities.sort_tables_by_schedule - "
                    "reason %s" % e.message)
                schedules = {}
        return schedules

    @classmethod
    def clean_lines(cls, lines):
        """strip space, newline characters and skip empty lines
        Args:
            lines: List of lines in a file. return value of open().readlines()
        """
        lines_str = ''.join(lines)
        lines = lines_str.splitlines()
        cleaned_lines = []
        for line in lines:
            if line.strip():
                cleaned_lines.append(line.strip())
        return cleaned_lines

    @classmethod
    def replace_special_chars(cls, value):
        """replace non alphanumerics except underscore to
        empty char in the given string
        """
        pattern_non_alphanumeric = re.compile(r'[^A-Za-z0-9_]')
        value = re.sub(pattern_non_alphanumeric, '', value)
        return value

    @classmethod
    def find_owner(cls, filename):
        """Fetch owner of the file
        Args:
            filename: absolute path
        """
        return getpwuid(os.stat(filename).st_uid).pw_name


class WorkflowTablesMapper(object):
    """Workflow and table names for printing"""

    def __init__(self, table_id):
        """init"""
        self.table_id = table_id
        self.view_table_names = '--x--x--'
        self.incr_wf = '--x--x--'
        self.full_wf = '--x--x--'
        self.is_subwf_wf = 'No'


class SafeDict(dict):
    """Used to catch missing keys in string format. By falsetru found
    at http://stackoverflow.com/questions/17215400/
    python-format-string-unused-named-arguments"""

    def __missing__(self, key):
        return '{' + key + '}'
