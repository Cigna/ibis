import os
from ibis.custom_logging import get_logger
from pkg_resources import resource_filename
from mako.template import Template


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class Control(object):

    def __init__(self, action_type, name, cfg_mgr):
        """Init."""
        self.action_type = action_type
        self.name = name
        self.cfg_mgr = cfg_mgr
        self.logger = get_logger(self.cfg_mgr)

    def get_action_type(self):
        """Return the type of action.
        :return String
        """
        return self.action_type

    def get_name(self):
        """Return the name of the action.
        :return String
        """
        return self.name

    def generate_action(self):
        """Return XML workflow control."""
        template_file = resource_filename('resources.templates',
                                          '{action}.xml.mako'.
                                          format(action=self.action_type))
        template = Template(filename=template_file, format_exceptions=True)
        return template.render(node=self)
