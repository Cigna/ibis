"""Action definitions."""
from pkg_resources import resource_filename
from ibis.utilities.utilities import *
from ibis.custom_logging import get_logger

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class Action(object):

    def __init__(self, action_type, name, ok, error, cfg_mgr):
        self.action_type = action_type
        self.name = name
        self.ok = ok
        self.error = error
        self.cfg_mgr = cfg_mgr
        self.logger = get_logger(self.cfg_mgr)

    def get_action_type(self):
        """Returns the type of action
        :return String"""
        return self.action_type

    def get_name(self):
        """Returns the name of the action
        :return String"""
        return self.name

    def get_ok(self):
        """Returns the ok to for an action
        :return String"""
        return self.ok

    def get_error(self):
        """Returns the error to for an action
        :return String"""
        return self.error

    def generate_action(self):
        """Return XML workflow action."""
        template_file = resource_filename('resources.templates',
                                          '{action}.xml.mako'.
                                          format(action=self.action_type))
        template = Template(filename=template_file, format_exceptions=True)
        return template.render(node=self)
