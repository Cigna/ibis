import os
from ibis.model.action import Action


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class Sqoop(Action):
    """Sqoop action class."""

    def __init__(self, **params):
        self.params = params
        self.params['action_type'] = self.params['action_type'].lower()
        if self.params['action_type'] == 'sqoop':
            try:
                super(self.__class__,
                      self).__init__(self.params['action_type'],
                                     self.params['name'],
                                     self.params['ok'],
                                     self.params['error'],
                                     self.params['cfg_mgr'])
                self.command = self.params['command']
                # self.logger.info("Sqoop action successful")
            except KeyError, e:
                self.logger.error('Sqoop KeyError - reason "%s"' % str(e))
        else:
            self.logger.error('Invalid expecting sqoop action type')

    def get_delete(self):
        """Returns a list of path to delete
        :return List[String]"""
        return self.params.get('delete', [])

    def get_mkdir(self):
        """Returns a list of path to mkdir
        :return List[String]"""
        return self.params.get('mkdir', [])

    def get_config(self):
        """Return list of configuration mapping
        :return List[{name: _some_value, value: _some_value}]"""
        return self.params.get('config', [])

    def get_command(self):
        """Returns the sqoop command to run
        :return String"""
        return self.command

    def get_arg(self):
        """Return a list of command arg
        :return List[arg]"""
        return self.params.get('arg', [])

    def get_job_tracker(self):
        """Returns the job tracker
        :return String"""
        return self.params.get('job_tracker', '')

    def get_name_node(self):
        """Returns the name node
        :return String"""
        return self.params.get('name_node', '')

    def get_file(self):
        """Returns a list of files
        :return List[String]"""
        return self.params.get('file', [])

    def get_archive(self):
        """Returns a list of archive
        :return List[String]"""
        return self.params.get('archive', [])
