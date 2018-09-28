import os
from ibis.model.action import Action


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class SSH(Action):

    def __init__(self, **params):
        self.params = params
        self.params['action_type'] = self.params['action_type'].lower()
        self.host = params['host']
        self.user = params['user']
        if self.params['action_type'] == 'ssh':
            try:
                super(self.__class__,
                      self).__init__(self.params['action_type'],
                                     self.params['name'],
                                     self.params['ok'],
                                     self.params['error'],
                                     self.params['cfg_mgr'])
                self.execute = self.params['execute']
            except KeyError, e:
                self.logger.error('Ssh KeyError - reason "%s"' % str(e))
        else:
            self.logger.error('Invalid expecting Ssh action type')

    def get_execute(self):
        """Returns script to execute
        :return String"""
        return self.execute

    def get_host(self):
        """Returns host"""
        return self.host

    def get_user(self):
        """Returns host"""
        return self.user

    def get_args(self):
        """Returns a list of args
        :return List[String]"""
        return self.params.get('args', [])

    def get_capture_output(self):
        """Returns true or false for capture output setting
        :return Boolean"""
        capture = True \
            if self.params.get('capture_output', '').lower() == 'true' \
            else False
        return capture
