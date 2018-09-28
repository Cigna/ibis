"""Insert workflow run time stats to default.oozie_checks_balances table"""
import sys
import json
import re
import os
import datetime
from subprocess import Popen, PIPE
import subprocess

import requests
from requests_kerberos import HTTPKerberosAuth
from py_hdfs import PyHDFS
from impala_utils import ImpalaConnect

_PIPE = '|'


class Action(object):
    """Action model"""

    def __init__(self, action_json):
        self.__dict__.update(json.loads(action_json))

    def get_status(self):
        return self.status

    def get_retries(self):
        return self.retries

    def get_transition(self):
        return self.transition

    def get_stats(self):
        return self.stats

    def get_start_time(self):
        return self.startTime

    def get_to_string(self):
        return self.toString

    def get_cred(self):
        return self.cred

    def get_error_message(self):
        return self.errorMessage

    def get_error_code(self):
        return self.errorCode

    def get_console_url(self):
        return self.consoleUrl

    def get_external_id(self):
        return self.externalId

    def get_external_status(self):
        return self.externalStatus

    def get_conf(self):
        return self.conf

    def get_type(self):
        return self.type

    def get_tracker_uri(self):
        return self.trackerUri

    def get_external_child_ids(self):
        return self.externalChildIDs

    def get_end_time(self):
        return self.endTime

    def get_data(self):
        return self.data

    def get_id(self):
        return self.id

    def get_name(self):
        return self.name

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False


class Workflow(object):
    """Workflow model"""

    def __init__(self, workflow_json):
        self.__dict__.update(json.loads(workflow_json))

    def get_status(self):
        return self.status

    def get_run(self):
        return self.run

    def get_start_time(self):
        return self.startTime

    def get_app_name(self):
        return self.appName

    def get_last_modified(self):
        return self.lastModTime

    def get_actions(self):
        actions = []
        for act in self.actions:
            actions.append(Action(json.dumps(act)))
        return actions

    def get_acl(self):
        return self.acl

    def get_app_path(self):
        return self.appPath

    def get_external_id(self):
        return self.externalId

    def get_console_url(self):
        return self.consoleUrl

    def get_conf(self):
        return self.conf

    def get_parent_id(self):
        return self.parentId

    def get_created_time(self):
        return self.createdTime

    def get_to_string(self):
        return self.toString

    def get_end_time(self):
        return self.endTime

    def get_id(self):
        return self.id

    def get_group(self):
        return self.group

    def get_user(self):
        return self.user

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False


class Job(object):
    """Job model"""

    def __init__(self, job_json):
        self.__dict__.update(json.loads(job_json))

    def get_offset(self):
        return self.offset

    def get_total(self):
        return self.total

    def get_len(self):
        return self.len

    def get_workflows(self):
        workflows = []
        for job in self.workflows:
            workflows.append(Workflow(json.dumps(job)))
        return workflows

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False


class ChecksBalancesExportHelper(object):
    def __init__(self, oozie_url):
        self.oozie_url = oozie_url

    def get_jobs(self, name=None, user=None, group=None, status=None,
                 offset=1, length=50, job_type='wf'):
        """Retrieve all jobs or filtered jobs
        :param name:String The application name from the
        workflow/coordinator/bundle definition
        :param user:String The user that submitted the job
        :param group:String The group for the job
        :param status:String The status of the job (KILLED, SUCCEEDED, RUNNING)
        :param offset:Int Parameter can be used for pagination
        :param length:Int Parameter can be used for pagination
        :param job_type:String Parameter for job type (wf,
        coordinator or bundle)
        :return status_code:Int, job:Job
        """
        job = None
        # Create query
        query = 'jobs'
        if name or user or group or status or offset or length or job_type:
            query += '?filter='
            if name:
                query += 'name={name}&'.format(name=name)
            if user:
                query += 'user={user}&'.format(user=user)
            if group:
                query += 'group={group}&'.format(group=group)
            if status:
                query += 'status={status}&'.format(status=status)
            if offset:
                query += 'offset={offset}&'.format(offset=offset)
            if length:
                query += 'len={length}&'.format(length=length)
            if job_type:
                query += 'jobtype={job_type}'.format(job_type=job_type)

        r = requests.get(self.oozie_url + query, auth=HTTPKerberosAuth())

        if r.status_code == 200:  # OK
            my_json = json.dumps(r.json())
            job = Job(my_json)
        else:
            print "Error retrieving all jobs. Error code: {status}".format(
                status=r.status_code)
        return r.status_code, job

    def get_job(self, id_val):
        """Retrieve job with provided id
        :param id_val:String
        :return workflow_job:Workflow"""
        workflow_job = None
        query = 'job/' + id_val
        r = requests.get(self.oozie_url + query, auth=HTTPKerberosAuth())
        if r.status_code == 200:  # OK
            my_json = json.dumps(r.json())
            workflow_job = Workflow(my_json)
        else:
            err_msg = "Error retrieving job, {id_val}. Error code: {status}"
            err_msg = err_msg.format(id_val=id_val, status=r.status_code)
            print err_msg
        return r.status_code, workflow_job


class ChecksBalancesExport(object):
    """Checks and balances model"""

    def __init__(self, domain, db, table_name, export_timestamp='',
                 row_count=0, push_time=0, directory='null',
                 txt_size=0, parquet_time=0, parquet_size=0, success='1'):
        self.domain = domain
        self.db = db
        self.table_name = table_name
        self.export_timestamp = export_timestamp
        self.row_count = row_count
        self.push_time = push_time
        self.directory = directory
        self.txt_size = txt_size
        self.parquet_time = parquet_time
        self.parquet_size = parquet_size
        self.success = success

    def set_domain(self, domain_name):
        self.domain = domain_name

    def set_db(self, db_name):
        self.db = db_name

    def set_table(self, table_name):
        self.table_name = table_name

    def set_export_timestamp(self, export_timestamp):
        self.export_timestamp = export_timestamp

    def set_row_count(self, row_count):
        self.row_count = row_count

    def set_push_time(self, push_time):
        self.push_time = push_time

    def set_directory(self, directory):
        self.directory = directory

    def set_txt_size(self, txt_size):
        self.txt_size = txt_size

    def set_parquet_time(self, parquet_time):
        self.parquet_time = parquet_time

    def set_parquet_size(self, parquet_size):
        self.parquet_size = parquet_size

    def set_success(self, success):
        self.success = success

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False


class ChecksBalancesExportManager(object):
    """Class used for managing the oozie_checks_balances table"""

    def __init__(self, host, oozie_url, chk_bal_exp_dir):
        self.host = host
        self.ooz = ChecksBalancesExportHelper(oozie_url)
        self.pyhdfs = PyHDFS(chk_bal_exp_dir)

    def get_workflow_job(self, app_name, start_time=None):
        """Given an app name and start time, use the oozie
        web services api to query for the job
        :param app_name: String Name of job
        :param start_time: String UTC current date and time in W3C format down
                           to the second (YYYY-MM-DDThh:mm:ss.sZ).
                           I.e.: 1997-07-16T19:20:30.45Z
        :param end_time: String UTC current date and time in W3C format down
                         to the second (YYYY-MM-DDThh:mm:ss.sZ).
        :return job: Job
        """
        workflow_job = None
        status, job = self.ooz.get_jobs(name=app_name)

        if status != 200:
            # Not OK
            err_msg = "Error, {status}, retrieving app name, {app_name}."
            err_msg = err_msg.format(status=status, app_name=app_name)
            print err_msg
        else:
            workflows = job.get_workflows()

            if len(workflows) > 1:
                if start_time:  # Return job id closest to start time
                    start = datetime.datetime.strptime(start_time,
                                                       "%Y-%m-%dT%H:%M:%S.%fZ")
                    # Use workflow[0] as threshold
                    workflow_time = datetime.datetime.strptime(
                        workflows[0].get_start_time(),
                        '%a, %d %b %Y %H:%M:%S %Z')
                    time_match = (workflow_time - start).total_seconds()
                    job_id = workflows[0].get_id()

                    for workflow in workflows:
                        workflow_time = datetime.datetime.strptime(
                            workflow.get_start_time(),
                            '%a, %d %b %Y %H:%M:%S %Z')
                        if workflow_time >= start and \
                                time_match >= \
                                (workflow_time - start).total_seconds():
                            job_id = workflow.get_id()
                else:
                    job_id = workflows[0].get_id()  # Return first result
            else:
                job_id = workflows[0].get_id()  # Return only result
            status, workflow_job = self.ooz.get_job(
                job_id)  # Get workflow job matching id

        return workflow_job

    def get_table_name(self, action):
        """Used to get the table name from the export_prep action
        :param action
        :return table_name: String"""
        table_name = None
        if action.get_type() == 'shell' and 'export_prep' in action.get_name():
            try:
                pattern = r'<env-var>table_name=(.*?)</env-var>'
                table_name = re.findall(pattern, action.get_conf(), re.DOTALL)[
                    0]
            except IndexError as ie:
                table_name = None
                err_msg = "Error found in oozie_ws_help.get_table_" \
                          "name - reason %s" % ie.message
                raise Exception(err_msg)
        return table_name

    def get_db_name(self, action):
        """Used to get the db name from the export_prep action
        :param action
        :return db_name: String"""
        db_name = None
        if action.get_type() == 'shell' and 'export_prep' in action.get_name():
            try:
                pattern = r'<env-var>database=(.*?)</env-var>'
                db_name = re.findall(pattern, action.get_conf(), re.DOTALL)[0]

            except IndexError as ie:
                db_name = None
                raise Exception(
                    "Error found in oozie_ws_help.get_db_name - "
                    "reason %s" % ie.message)
        return db_name

    def get_domain_name(self, action):
        """Used to get the domain from export_prep action
        :param action
        :return domain_name: String"""
        domain_name = None
        if action.get_type() == 'shell' and 'export_prep' in action.get_name():
            try:
                pattern = r'<env-var>database=(.*?)</env-var>'
                domain_name = re.findall(pattern, action.get_conf(),
                                         re.DOTALL)[0]
            except IndexError as ie:
                domain_name = None
                err_msg = "Error found in oozie_ws_help.get_domain" \
                          "_name - reason %s" % ie.message
                raise Exception(err_msg)
        return domain_name

    def get_input_records(self, sqoop_action):
        """Used to get the number of input records
        :param action
        :return records:String"""
        records = 0
        if sqoop_action.get_type() == 'sqoop' \
                and sqoop_action.get_status() == "OK":
            try:
                pattern = r'\"MAP_INPUT_RECORDS\":(.*?),'
                records = \
                    re.findall(pattern, sqoop_action.get_stats(), re.DOTALL)[0]
            except IndexError as ie:
                records = None
                raise Exception(
                    "Error found in "
                    "checks_and_balances_export.get_input_records "
                    "- reason %s" % ie.message)
        else:
            print "Error. Expecting sqoop action and OK status"
        return records

    def get_output_records(self, sqoop_action):
        """Used to get the number of output records
        :param action
        :return records:String"""
        records = 0
        if sqoop_action.get_type() == 'sqoop' \
                and sqoop_action.get_status() == "OK":
            try:
                pattern = r'\"MAP_OUTPUT_RECORDS\":(.*?),'
                records = \
                    re.findall(pattern, sqoop_action.get_stats(), re.DOTALL)[0]
            except IndexError as ie:
                records = None
                err_msg = "Error found in checks_and_balances_export." \
                          "get_output_records - reason %s"
                err_msg = err_msg % ie.message
                raise Exception(err_msg)
        else:
            print "Error. Expecting sqoop action and OK status"
        return records

    def get_sqoop_duration(self, sqoop_action):
        """Returns how long it took to sqoop
        :param sqoop_action:Action
        :return time:String"""
        time = 0
        if sqoop_action.get_type() == 'sqoop' \
                and sqoop_action.get_status() == "OK":
            start = datetime.datetime.strptime(
                sqoop_action.get_start_time(), '%a, %d %b %Y %H:%M:%S %Z')
            end = datetime.datetime.strptime(
                sqoop_action.get_end_time(), '%a, %d %b %Y %H:%M:%S %Z')
            time = (end - start).total_seconds()
        else:
            print "Error. Expecting sqoop action and OK status"
        return int(time)

    def get_directory(self, action):
        """Used to retrieve the target_dir in the conf if it
        exists otherwise None
        :param action:Action
        :return directory:String"""
        directory = None
        try:
            pattern = r'<env-var>source_dir=(.*?)</env-var>'
            directory = re.findall(pattern, action.get_conf(), re.DOTALL)[0]
        except IndexError as ie:
            directory = None
            err_msg = "Error found in oozie_ws_help.get_directory - reason %s"
            err_msg = err_msg % ie.message
            raise Exception(err_msg)
        return directory

    def get_txt_size(self, sqoop_action):
        """Used to retrieve the avro size if it exists otherwise None
        Note: for incremental, data is not loaded in as "avro" but the same
        method below is used as it is looking for HDFS_BYTES_READ
        :param sqoop_action:Action
        :return size:String"""
        size = 0
        if sqoop_action.get_type() == 'sqoop' \
                and sqoop_action.get_status() == "OK":
            try:
                pattern = r'\"HDFS_BYTES_READ\":(.*?),'
                size = re.findall(pattern, sqoop_action.get_stats(),
                                  re.DOTALL)[0]
            except IndexError as ie:
                size = None
                err_msg = "Error found in oozie_ws_help.get_txt_size - " \
                          "reason %s"
                err_msg = err_msg % ie.message
                raise Exception(err_msg)
        else:
            print "Error. Expecting sqoop action and OK status"
        return size

    def get_parquet_time(self, table_name, actions):
        """Finds the parquet time if available.

        There are two types of parquet times, one for full loads, the other
        for incremental.

        For full:
        Parquet step involves the following workflow actions,
        {name}_avro -> {name}_avro_parquet -> {name}_parquet_swap ->
        {name}_parquet_live), and is calculated by calculating the time
        difference when it begins the _avro action and finishes
        the _parquet_live action

        For incremental:
        The parquet step happens in the _merge_partition, where the data that
        has been loaded is moved into the final (parquet) table.

        :param table_name: String
        :param actions:List[Action]
        """
        start_time = None
        end_time = None
        parquet_time = 0

        for action in actions:
            if '_parquet_text' in action.get_name():
                if action.get_name() == table_name + '_parquet_text':
                    start_time = datetime.datetime.strptime(
                        action.get_start_time(), '%a, %d %b %Y %H:%M:%S %Z')
                if action.get_name() == table_name + '_parquet_live':
                    end_time = datetime.datetime.strptime(
                        action.get_end_time(), '%a, %d %b %Y %H:%M:%S %Z')
            elif '_merge_partition' in action.get_name():
                start_time = datetime.datetime.strptime(
                    action.get_start_time(), '%a, %d %b %Y %H:%M:%S %Z')
                end_time = datetime.datetime.strptime(
                    action.get_end_time(), '%a, %d %b %Y %H:%M:%S %Z')
        if start_time and end_time:
            parquet_time = (end_time - start_time).total_seconds()
        else:
            parquet_time = 0
        return int(parquet_time)

    def get_distinct_workflow_tables(self, workflow_job):
        """Given a workflow job, query the actions and find
         all distinct tables"""
        actions = workflow_job.get_actions()
        actions_new = actions
        sub_actions = ""
        # Assuming all sub-workflows are called job_0, job_1, job_2, etc
        normal_or_sub = False
        job_numbers = {}
        for action in actions:
            if 'sub-workflow' in action.get_type():
                normal_or_sub = True
                # get name of job (job_0, etc)
                job_numbers[action.get_name()] = action.get_external_id()

        if normal_or_sub:
            sub_workflow_to_update = job_numbers[max(job_numbers.keys())]
            id_externals = self.ooz.get_job(sub_workflow_to_update)
            sub_actions = id_externals[1].get_actions()
            actions_new = sub_actions

        export_preps = [action for action in actions_new if
                        action.get_type() == 'shell' and
                        'export_prep.sh' in action.get_conf()]

        pattern = r'<env-var>table_name=(.*?)</env-var>'
        tables = []
        for action in export_preps:
            conf = action.get_conf()
            lst = re.findall(pattern, conf, re.DOTALL)
            if lst:
                tables.append(lst[0])
        return tables

    def sort_actions(self, workflow_job):
        """Given a workflow_job sort the actions by table
        :param workflow_job:Workflow
        :return sorted_actions: {table_name: Action}"""
        sorted_actions = {}
        tables = self.get_distinct_workflow_tables(workflow_job)

        # Prep keys, create empty list for each table name
        for table_name in tables:
            sorted_actions[table_name] = []

        actions = workflow_job.get_actions()  # List of dictionary of actions
        for action in actions:
            for k in sorted_actions.keys():
                if k in action.get_name():
                    action_list = sorted_actions[k]
                    action_list.append(action)
                    sorted_actions[k] = action_list
        return sorted_actions

    def get_export_prep_stats(self, action):
        """Utilize the export prep action to get the table name,
        domain name and database name
        :param action: Action
        :return stats: Dictionary"""
        stats = {}
        if action.get_type() == 'shell' and 'export_prep' in action.get_name():
            stats['domain'] = self.get_domain_name(action)
            stats['db'] = self.get_db_name(action)
            stats['table_name'] = self.get_table_name(action)
            stats['directory'] = self.get_directory(action)
        else:
            print 'Expecting an export prep action'
        return stats

    def validate_in_and_out_counts(self, action):
        """
        Used to validate that the in rows and the out rows
        count for the application are the same.
        :param action:
        :return: Row counts if they match
        """
        in_rows = self.get_input_records(action)
        out_rows = self.get_output_records(action)
        if in_rows == out_rows:
            return in_rows
        else:
            raise Exception

    def get_export_stats(self, action):
        """Utilize the export action to get metrics
        :param action: Action
        :return stats: Dictionary"""
        stats = {}
        if action.get_type() == 'sqoop' and '_export' in action.get_name():
            stats['row_count'] = self.validate_in_and_out_counts(action)
            stats['push_time'] = self.get_sqoop_duration(action)
            stats['export_timestamp'] = action.get_start_time()
            stats['txt_size'] = self.get_txt_size(action)
        else:
            print 'Expecting an export action.'
        return stats

    def get_parquet_size(self, table_name, target_dir):
        """Finds the parquet size. REQUIRES hadoop fs"""
        if target_dir.startswith('/user/data/incrementals'):
            data_dir = "/user/data/" + "/".join(
                target_dir.split('/')[4:]) + '/live'
        else:
            data_dir = '/user/data/' + target_dir + '/live'

        # Check if the dir exists
        dir_status = subprocess.call(["hadoop", "fs", "-test", "-e", data_dir])
        if dir_status == 0:
            proc = Popen(['hadoop', 'fs', '-ls', data_dir], stdout=PIPE)
            out, err = proc.communicate()

            # note all new ingestions have this partition, including
            # full ingest-onlys
            incr_ingest = "/incr_ingest_timestamp="
            date_list = []
            full_ingests = []
            output = out.split('\n')

            for line in output:
                all_date = line.split(incr_ingest)
                if len(all_date) > 1:
                    try:
                        date_list.append(
                            datetime.datetime.strptime(all_date[1],
                                                       '%Y-%m-%d'))
                    except ValueError:
                        if all_date[1].startswith('full_'):
                            full_ingests.append(all_date[1])

            if len(date_list) > 0:
                # take the max date as the directory to get the size of
                # by using the datetime, you can just call max(date)
                # see here for best way to format a date:
                # http://stackoverflow.com/questions/10624937/convert-
                # datetime-object-to-a-string-of-date-only-in-python
                parquet_dir = data_dir + incr_ingest + '{:%Y-%m-%d}'.format(
                    max(date_list))
            elif len(full_ingests) > 0:
                # Only full ingests, take size of entire /live dir
                parquet_dir = data_dir
            else:
                parquet_dir = data_dir

            parq_size = 0
            try:
                proc = Popen(['hadoop', 'fs', '-du', '-s', parquet_dir],
                             stdout=PIPE)
                out, err = proc.communicate()
                # 235583051  706749153  /user/data/mdm/member/fake_database/
                # fake_mem_tablename_risk_fact/live
                try:
                    parq_size = out.split()
                    if parq_size:
                        parq_size = parq_size[len(parq_size) - 3]
                    if not parq_size.isdigit():
                        parq_size = 0
                except IndexError as ie:
                    parq_size = 0
                    err_msg = "Parquet Size error found in oozie_ws_help.get" \
                              "_parquet_size - reason %s"
                    err_msg = err_msg % ie.message
                    raise Exception(err_msg)
            except OSError:
                parq_size = None
                err_msg = "OS Parquet Size error found in oozie_ws_help." \
                          "get_parquet_size - reason %s"
                err_msg = err_msg % ie.message
                raise Exception(err_msg)
            return parq_size
        return 0

    def get_success_or_failure(self, action_list):
        """
        Given actions, check to make sure all the actions suceeded to determine
        if entire workflow suceeded
        :param action_list: A list of actions for a workflow
        :return: int, success / failure depending on result of actions
        """
        actions = []
        for action in action_list:
            actions.append(action.get_status())
        if 'OK' in actions and len(set(actions)) == 1:
            return '0'
        else:
            return '2'

    def get_records(self, workflow_job):
        """Given a workflow_job return a list of ChecksBalances
        object per table
        :param workflow_job:Workflow
        :return records:Dictionary A dictionary containing values for the
        ChecksBalances object
        """
        # Find all distinct tables and sort all actions by table name as key
        sorted_actions = self.sort_actions(
            workflow_job)  # {table_name: [Action]}
        parser = {'export_prep': self.get_export_prep_stats,
                  'export': self.get_export_stats}

        # For each distinct table find required values and create
        #  ChecksBalances object
        cb_records = []
        for table_name in sorted_actions.keys():
            records = {}
            for key in parser.keys():
                for action in sorted_actions[table_name]:
                    if key in action.get_name():
                        record = parser[key](action)
                        if record:
                            records = dict(records, **record)

            records['parquet_time'] = self.get_parquet_time(table_name,
                                                            sorted_actions[
                                                                table_name])
            records['parquet_size'] = self.get_parquet_size(table_name,
                                                            records[
                                                                'directory'])
            records['success'] = self.get_success_or_failure(
                sorted_actions[table_name])
            cb_records.append(ChecksBalancesExport(**records))
        return cb_records

    def update_checks_balances(self, workflow, start_time=None):
        """Given a workflow app name, query oozie api for workflow job,
        update checks_balances table with records
        """
        if workflow:
            for record in self.get_records(workflow):
                # Update records to checks balances table
                # Parquet size is dependent on live location size in hdfs.
                # Records only get replaced in live if
                # successful otherwise keep last ingest records
                self.update_old_cb_table(
                    record.domain, record.table_name, record)

            # We need to perform INVALIDATE METADATA to reflect Hive
            # file-based operations in Impala.
            ImpalaConnect.invalidate_metadata(self.host,
                                              'ibis.checks_balances_export')

    def update_old_cb_table(self, domain, table_name, record):
        """Update current_repull value in ibis.checks_balances """
        # File-based insert/update operation
        pipe_seperated_value = self.prepares_data(record)
        self.pyhdfs.insert_update(
            self.get_dir_path(
                domain,
                table_name),
            pipe_seperated_value)

    def prepares_data(self, record):
        pipe_seperated_value = str(record.directory) + _PIPE
        pipe_seperated_value += str(record.push_time) + _PIPE
        pipe_seperated_value += str(record.txt_size) + _PIPE
        pipe_seperated_value += str(record.export_timestamp) + _PIPE
        pipe_seperated_value += str(record.parquet_time) + _PIPE
        pipe_seperated_value += str(record.parquet_size) + _PIPE
        pipe_seperated_value += str(record.row_count) + _PIPE
        pipe_seperated_value += 'null' + _PIPE
        pipe_seperated_value += 'null' + _PIPE
        pipe_seperated_value += 'null' + _PIPE
        pipe_seperated_value += str(record.success) + _PIPE
        pipe_seperated_value += 'null'
        return pipe_seperated_value

    def get_dir_path(self, domain, table):
        return '/domain={0}/table_name={1}'.format(domain, table)

    def check_if_workflow(self, app_name):
        workflow_job = self.get_workflow_job(app_name)
        actions = workflow_job.get_actions()
        actions_new = actions
        workflow = ""
        # Assuming all sub-workflows are called job_0, job_1, job_2, etc
        normal_or_sub = False
        sub_actions = ""
        job_numbers = {}
        for action in actions:
            if 'sub-workflow' in action.get_type():
                normal_or_sub = True
                # get name of job (job_0, etc)
                job_numbers[action.get_name()] = action.get_external_id()
        sub_actions = ""
        if normal_or_sub:
            sub_workflow_to_update = job_numbers[max(job_numbers.keys())]
            id_externals = self.ooz.get_job(sub_workflow_to_update)
            workflow = id_externals[1]
            sub_actions = id_externals[1].get_actions()
        else:
            workflow = workflow_job
        return workflow

    def check_if_workflow_actions(self, app_name):
        workflow_job = self.get_workflow_job(app_name)
        actions = workflow_job.get_actions()
        print "actions", actions
        return actions

if __name__ == "__main__":
    # Application name
    app_name = sys.argv[1]
    HOST = os.environ['IMPALA_HOST']
    OOZIE_URL = sys.argv[2]
    CHK_BAL_EXP_DIR = sys.argv[3]
    export_cb_mgr = ChecksBalancesExportManager(
        HOST,
        OOZIE_URL,
        CHK_BAL_EXP_DIR)
    app_name = export_cb_mgr.check_if_workflow(app_name)
    export_cb_mgr.update_checks_balances(app_name)
    ImpalaConnect.close_conn()
