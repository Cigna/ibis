"""Config Manager."""
import ConfigParser
import os
import sys
import random
import string
from pkg_resources import resource_filename


class ConfigManager(object):
    """Ibis properties value manager.
    Reads from properties file and values can be accessed
    via the Config Manager. Requires path to properties file
    """

    def __init__(self, environment, *args):
        """Init."""

        if environment == "JENKINS":
            self.env = "dev"
        else:
            self.env = environment.lower()

        self.for_env = None
        if args:
            self.for_env = args[0]
            if self.for_env:
                self.for_env = self.for_env[0]

        env_prop_file = '{env}.properties'.format(env=environment.lower())
        prop_path = resource_filename('resources', env_prop_file)
        config = ConfigParser.RawConfigParser()
        config.read(prop_path)

        # This is the base dir of this particular file
        # for example:
        # /Users/USERNAME/Desktop/ibis/dist/Ibis-0.3.0-py2.7.egg
        # /ibis/utilities/config_manager.pyc
        # we then take up a few knotches, to just get
        # /Users/USERNAME/Desktop/ibis/
        # This allows us to not have multiple property files for Jenkins
        # (for their different envs)
        # Going forward, we want to make this part of all the properties
        # files and have Jenkins build the files for us
        # Going with this, check out the jenkins.properties file
        # for how it's now not harded-coded paths
        curr_path = os.path.abspath(__file__)
        for _ in range(4):
            # cross platform path split
            curr_path = os.path.split(curr_path)[0]
        self.base_dir = curr_path

        # Used for selecting where workflows should be commited to with GIT
        if environment in ["JENKINS", "UNIT_TEST", 'INT_TEST']:
            self.saves = self.base_dir + config.get('Directories', 'saves')
        else:
            self.saves = config.get('Directories', 'saves')
        # Create path for workflows
        self.create_path(self.saves)

        # Database
        # the host to connect for it_table
        self.host = config.get('Database', 'host')
        self.port = int(config.get('Database', 'port'))
        self.it_table = config.get('Database', 'it_table')
        self.it_table_export = config.get('Database', 'it_table_export')
        self.staging_database = config.get('Database', 'staging_database')
        self.use_kerberos = config.get('Database', 'use_kerberos')
        self.checks_balances = config.get('Database', 'checks_balances')
        self.esp_ids_table = config.get('Database', 'esp_ids_table')
        self.staging_it_table = config.get('Database', 'staging_it_table')
        self.prod_it_table = config.get('Database', 'prod_it_table')
        self.queue_name = config.get('Database', 'queue_name')
        self.edge_node = config.get('Database', 'edge_node')

        # Workflows
        # the host on which we run the workflow
        self.workflow_host = config.get('Workflows', 'workflow_host')
        self.workflow_hive2_jdbc_url = config.get(
            'Workflows', 'workflow_hive2_jdbc_url')
        self.default_db_env = config.get('Workflows', 'db_env').lower()
        self.kerberos = config.get('Workflows', 'kerberos')
        self.hdfs_ingest_version = config.get(
            'Workflows', 'hdfs_ingest_version')

        rand_dir_name = self.rand_name()
        # Directories
        self.logs = self.base_dir + config.get('Directories', 'logs') + \
            rand_dir_name
        self.log_file = self.logs + '/ibis.log'
        self.files = self.base_dir + config.get('Directories', 'files') + \
            rand_dir_name + '/'
        self.create_path(self.logs)
        self.create_path(self.files)
        self.custom_scripts_shell = config.get(
            'Directories', 'custom_scripts_shell')
        self.custom_scripts_hql = config.get(
            'Directories', 'custom_scripts_hql')
        self.requests_dir = config.get('Directories', 'requests_dir')
        self.export_hdfs_root = config.get('Directories', 'export_hdfs_root')
        self.root_hdfs_saves = config.get('Directories', 'root_hdfs_saves')
        self.kite_shell_name = config.get('Directories', 'kite_shell_name')
        self.kite_shell_dir = config.get('Directories', 'kite_shell_dir')

        # Templates
        self.start_template = resource_filename(
            'resources.templates', config.get('Templates', 'start_workflow'))
        self.end_template = resource_filename(
            'resources.templates', config.get('Templates', 'end_workflow'))
        self.export_end_template = resource_filename(
            'resources.templates', config.get('Templates',
                                              'export_end_workflow'))
        self.sub_workflow_template = resource_filename(
            'resources.templates', config.get('Templates', 'sub_workflow'))
        self.korn_shell_template = resource_filename(
            'resources.templates', config.get('Templates', 'korn_shell'))
        self.job_prop_template = resource_filename(
            'resources.templates', config.get('Templates', 'job_properties'))
        self.job_config_xml_template = resource_filename(
            'resources.templates', 'job_config.xml.mako')
        self.export_to_td_template = resource_filename(
            'resources.templates', config.get('Templates', 'export_to_td'))
        self.wld_template_mako = resource_filename(
            'resources.templates', 'esp_job.wld.mako')
        self.fake_end_template = resource_filename(
            'resources.templates', config.get('Templates', 'fake_end_workflow'))

        # Mappers
        self.oracle_mappers = self.gen_dict(
            config.get('Mappers', 'oracle_mappers'))
        self.teradata_mappers = self.gen_dict(
            config.get('Mappers', 'teradata_mappers'))
        self.db2_mappers = self.gen_dict(config.get('Mappers', 'db2_mappers'))
        self.sqlserver_mappers = self.gen_dict(
            config.get('Mappers', 'sqlserver_mappers'))
        self.sqlserver_mappers = self.gen_dict(
            config.get('Mappers', 'postgresql_mappers'))
        self.mysql_mappers = self.gen_dict(config.get('Mappers',
                                                      'mysql_mappers'))

        # Oozie
        self.oozie_url = config.get('Oozie', 'oozie_url')
        self.oozie_workspace = config.get('Oozie', 'workspace')
        self.hadoop_credstore_password_disable = config.get(
            'Oozie', 'hadoop_credstore_password_disable')
        if self.hadoop_credstore_password_disable is None:
            self.hadoop_credstore_password_disable = False
        elif self.hadoop_credstore_password_disable == 'True':
            self.hadoop_credstore_password_disable = True
        else:
            self.hadoop_credstore_password_disable = False

        self.hive_workspace = config.get('Oozie', 'hql_workspace')
        self.shell_workspace = config.get('Oozie', 'shell_workspace')
        self.impala_workspace = config.get('Oozie', 'impala_workspace')
        self.hql_views_workspace = config.get('Oozie', 'hql_views_workspace')

        # ESP_ID
        self.big_data = config.get('ESP_ID', 'big_data')
        # Dictionary of value:value_name
        self.frequencies_map = self.gen_dict(
            config.get('ESP_ID', 'frequencies_map'))
        self.environment_map = config.get('ESP_ID', 'environment_map')
        self.from_branch = config.get('ESP_ID', 'from_branch')

        # Other
        # Dictionary of value:value_name
        self.allowed_frequencies = self.gen_dict(
            config.get('Other', 'allowed_frequencies'))
        self.vizoozie = resource_filename('resources',
                                          config.get('Other', 'vizoozie'))
        self.max_table_per_workflow = int(
            config.get('Other', 'max_table_per_workflow'))
        self.parallel_dryrun_procs = int(config.get(
            'Other', 'parallel_dryrun_procs'))
        self.parallel_sqoop_procs = int(config.get(
            'Other', 'parallel_sqoop_procs'))
        self.domain_suffix = config.get('Other', 'domain_suffix')
        self.teradata_server = self.gen_dict(
            config.get('Other', 'teradata_server'))

        self.domains_list = config.get('Other', 'domains_list')

        if self.for_env:
            self.override_env()
        else:
            self.for_env = self.env

        self.beeline_url = self.workflow_hive2_jdbc_url

        self.freq_ingest = config.get('Database', 'freq_ingest')

    def override_env(self):
        """To generate workflow for dev/int on prod."""
        if 'dev' in self.for_env.lower() or 'int' in self.for_env.lower():
            # to make wf generation for all envs possible on prod host
            env_prop_file = '{env}.properties'.format(env=self.for_env.lower())
            prop_path = resource_filename('resources', env_prop_file)
            config = ConfigParser.RawConfigParser()
            config.read(prop_path)
            # override the values
            self.it_table = config.get('Database', 'it_table')
            self.oozie_url = config.get('Oozie', 'oozie_url')
            self.kerberos = config.get('Workflows', 'kerberos')
            self.allowed_actions = config.get('Workflows', 'actions')
            self.hdfs_ingest_version = config.get('Workflows',
                                                  'hdfs_ingest_version')
            self.job_prop_template = resource_filename(
                'resources.templates', config.get('Templates',
                                                  'job_properties'))
            self.edge_node = config.get('Database', 'edge_node')

    def gen_dict(self, mapping):
        dictionary = {}
        for kv in mapping.split(','):
            key, val = kv.split(':')
            dictionary[key.strip()] = val.strip()
        return dictionary

    def rand_name(self):
        """creates random string"""
        val = ''.join(random.choice(string.lowercase) for i in range(10))
        return val

    def create_path(self, path):
        """Create path when not exists"""
        try:
            file_permission = 0774
            if not os.path.exists(path):
                os.makedirs(path)
                os.chmod(path, file_permission)
        except Exception as e:
            print "Error: could not create workflow directory, details %s" % e
            print 'Check for unix permissions. Exiting now...'
            sys.exit(1)

    def read_config_wf_props(self, config_workflow_properties):
        """Reads config workflow properties
        Args:
            config_workflow_properties: properties file
        """
        prop_file = '{config_wf_props}.properties'.format(
            config_wf_props=config_workflow_properties.lower())
        prop_path = resource_filename('resources', prop_file)
        config = ConfigParser.RawConfigParser()
        config.read(prop_path)
        return config

    def __getstate__(self):
        result = self.__dict__.copy()
        return result

    def __repr__(self):
        """Obj repr."""
        text = ''
        for data_attribute, value in vars(self).items():
            text += "{0}: {1}\n".format(*(data_attribute, value))
        return text
