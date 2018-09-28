import os
from ibis.model.action import Action


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class Hive(Action):
    """Hive action class."""

    def __init__(self, **params):
        """Init."""
        self.params = params
        self.params['action_type'] = self.params['action_type'].lower()
        if self.params['action_type'] == 'hive':
            try:
                super(self.__class__,
                      self).__init__(self.params['action_type'],
                                     self.params['name'],
                                     self.params['ok'],
                                     self.params['error'],
                                     self.params['cfg_mgr'])
                self.script = self.params['script']
                # self.logger.info("Hive action successful")
            except KeyError, e:
                self.logger.error('Hive KeyError - reason "%s"' % str(e))
        else:
            self.logger.error('Invalid expecting hive action type')

    def get_job_tracker(self):
        """Returns the job tracker
        :return String"""
        return self.params.get('job_tracker', '')

    def get_name_node(self):
        """Returns the name node
        :return String"""
        return self.params.get('name_node', '')

    def get_script(self):
        """Returns the script
        :return String"""
        return self.script

    def get_delete(self):
        """Return paths to delete
        :return List[String]"""
        return self.params.get('delete', [])

    def get_mkdir(self):
        """Return paths to mkdir
        :return List[String]"""
        return self.params.get('mkdir', [])

    def get_job_xml(self):
        """Return a list of job xml
        :return List[String]"""
        return self.params.get('job_xml', [])

    def get_config(self):
        """Return list of configuration
        :return List[{name: _some_value, value: _some_value}"""
        return self.params.get('config', [])

    def get_param(self):
        """Return a list of param
        :return List[String]"""
        return self.params.get('param', [])

    def get_file(self):
        """Return a list of files
        :return List[String]"""
        return self.params.get('file', [])

    def get_archive(self):
        """Return a list of archive
        :return List[String]"""
        return self.params.get('archive', [])
