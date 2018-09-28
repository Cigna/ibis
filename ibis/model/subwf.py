import os
from ibis.model.action import Action


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class SubWF(Action):
    """Sub workflow action class"""
    def __init__(self, **params):
        self.params = params
        self.params['action_type'] = self.params['action_type'].lower()
        if self.params['action_type'] == 'subwf':
            try:
                super(self.__class__,
                      self).__init__(self.params['action_type'],
                                     self.params['name'],
                                     self.params['ok'],
                                     self.params['error'],
                                     self.params['cfg_mgr'])
            except KeyError, e:
                self.logger.error('Shell KeyError - reason "%s"' % str(e))
        else:
            self.logger.error('Invalid expecting shell action type')

    def get_config(self):
        """Return list of configuration mapping
        :return List[{name: _some_value, value: _some_value}]"""
        return self.params.get('config', [])

    def get_path(self):
        """Returns a list of file paths
        :return List[String]"""
        return self.params.get('file', '')
