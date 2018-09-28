import os
from ibis.model.action import Action


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class Shell(Action):
    """Shell action class."""

    def __init__(self, **params):
        self.params = params
        self.params['action_type'] = self.params['action_type'].lower()
        if self.params['action_type'] == 'shell':
            try:
                super(self.__class__,
                      self).__init__(self.params['action_type'],
                                     self.params['name'],
                                     self.params['ok'],
                                     self.params['error'],
                                     self.params['cfg_mgr'])
                self.execute = self.params['execute']
                # self.logger.info("Shell action Executed Successfully")
            except KeyError, e:
                self.logger.error('Shell KeyError - reason "%s"' % str(e))
        else:
            self.logger.error('Invalid expecting shell action type')

    def get_execute(self):
        """Returns script to execute
        :return String"""
        return self.execute

    def get_delete(self):
        """Return paths to delete
        :return List[String]"""
        return self.params.get('delete', [])

    def get_mkdir(self):
        """Return paths to mkdir
        :return List[String]"""
        return self.params.get('mkdir', [])

    def get_config(self):
        """Return list of configuration mapping
        :return List[{name: _some_value, value: _some_value}]"""
        return self.params.get('config', [])

    def get_arg(self):
        """Returns a list of args
        :return List[String]"""
        return self.params.get('arg', [])

    def get_env_var(self):
        """Returns a list of environmental variables
        :return List[{VAR=VALUE}]"""
        return self.params.get('env_var', [])

    def get_file(self):
        """Returns a list of file paths
        :return List[String]"""
        return self.params.get('file', [])

    def get_archive(self):
        """Returns a list of file paths
        :return List[String]"""
        return self.params.get('archive', [])

    def get_capture_output(self):
        """Returns true or false for capture output setting
        :return Boolean"""
        capture = True if \
            self.params.get('capture_output', '').lower() == 'true' else False
        return capture
