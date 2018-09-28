import os
from ibis.model.control import Control


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class Join(Control):
    """Join control class."""

    def __init__(self, **params):
        self.params = params
        self.params['action_type'] = self.params['action_type'].lower()
        if self.params['action_type'] == 'join':
            try:
                super(self.__class__,
                      self).__init__(self.params['action_type'],
                                     self.params['name'],
                                     self.params['cfg_mgr'])
            except KeyError, e:
                self.logger.error('Join KeyError - reason "%s"' % str(e))
        else:
            self.logger.error('Invalid expecting join control type')

    def get_to_node(self):
        """Return to node
        :return String"""
        return self.params.get('to_node', '')
