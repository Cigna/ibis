import os
from ibis.model.control import Control


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class Fork(Control):
    """Fork control class."""

    def __init__(self, **params):
        self.params = params
        self.params['action_type'] = self.params['action_type'].lower()
        if self.params['action_type'] == 'fork':
            try:
                super(self.__class__,
                      self).__init__(self.params['action_type'],
                                     self.params['name'],
                                     self.params['cfg_mgr'])
                self.logger.info("Successfully forked")
            except KeyError, e:
                self.logger.error('Fork KeyError - reason "%s"' % str(e))
        else:
            self.logger.error('Invalid expecting fork control type')

    def get_to_nodes(self):
        """Return a list of node names.

        :return List[String]
        """
        return self.params.get('to_nodes', [])
