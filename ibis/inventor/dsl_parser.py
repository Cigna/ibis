"""Parses DSL file and creates custom workflows"""
import copy
import os
import pyparsing as pp
from ibis.utilities.utilities import Utilities
from ibis.custom_logging import get_logger


class WorkflowRule(object):
    """Workflow rules based on custom dsl config"""

    def __init__(self, action_id):
        """init"""
        self.metadata = {
            'action_id': action_id,
            'custom_action_script': ''
        }

    def get_meta_dict(self):
        """Returns the value associated with the metadata dict"""
        # caution: dict is mutable type
        return copy.deepcopy(self.metadata)

    @property
    def action_id(self):
        """Returns the action id"""
        val = self.get_meta_dict().get('action_id', '')
        return val

    @action_id.setter
    def action_id(self, val):
        """setter for action_id"""
        self.metadata['action_id'] = val

    @property
    def is_custom_action(self):
        """check if it is custom action"""
        val = False
        if self.get_meta_dict().get('custom_action_script', ''):
            val = True
        return val

    @property
    def is_hive_script(self):
        """check if its a custom hive script"""
        val = False
        script = self.get_meta_dict().get('custom_action_script', '')
        if script and '.hql' in script:
            val = True
        return val

    @property
    def custom_action_script(self):
        """Returns the custom_action_script file name"""
        val = self.get_meta_dict().get('custom_action_script', '')
        return val

    @custom_action_script.setter
    def custom_action_script(self, val):
        """setter for custom_action_script"""
        self.metadata['custom_action_script'] = val

    def __repr__(self):
        """Obj repr. Prints only python properties"""
        text = '\nWorkflow Rule:-'
        for attr in dir(self):
            if isinstance(getattr(type(self), attr, None),
                          property):
                msg = "\n{property_name}: {value}"
                text += msg.format(property_name=attr,
                                   value=getattr(self, attr))
        text += '\n====================='
        return text


class DSLParser(object):
    """Parser for ibis dsl"""

    def __init__(self, cfg_mgr, pre_defined_actions, scripts_dir):
        """init
        Args:
            cfg_mgr: Instance of ibis.utilities.config_manager.ConfigManager
            pre_defined_actions: list of default action ids
        """
        self.cfg_mgr = cfg_mgr
        self.logger = get_logger(self.cfg_mgr)
        self.pre_defined_actions = pre_defined_actions
        header_pattern = pp.Keyword('action').setResultsName('action_header')
        body_pattern = pp.Word(pp.alphanums + '._/')
        body_pattern = body_pattern.setResultsName('action_id')
        self.pattern = header_pattern + \
            pp.Group(body_pattern).setResultsName('action_body')
        self.scripts_dir = scripts_dir

    def _get_file_path(self, file_name):
        """Fetch full path based on requests dir
        Args:
            file_name: DSL file in ibis/request-files git repo
        """
        return os.path.join(self.scripts_dir, file_name)

    def parse_file(self, path):
        """parses DSL file and validates it
        Args:
            path: DSL file path
        Returns:
            action_data: list of action names from config file
        """
        with open(path, 'rb') as file_h:
            lines = file_h.readlines()
        lines = Utilities.clean_lines(lines)
        action_data = []
        for index, line in enumerate(lines):
            try:
                result = self.pattern.parseString(line)
                action_data.append(result['action_body']['action_id'])
            except pp.ParseException as parse_ex:
                err_msg = "Line {lineno}, column {err.col}:\n"
                err_msg += "Fix this line: '{err.line}'"
                err_msg = err_msg.format(err=parse_ex, lineno=index+1)
                err_msg = Utilities.print_box_msg(err_msg, border_char='x')
                raise ValueError(err_msg)
        return action_data

    def generate_rules(self, action_data):
        """Generates workflow rules based on action ids
        Args:
            action_data: list of action ids
        Returns:
            rules: list of [ibis.inventor.dsl_parser.WorkflowRule]
        """
        rules = []
        for action_id in action_data:
            if action_id in self.pre_defined_actions:
                wf_rule = WorkflowRule(action_id)
            elif '.hql' in action_id:
                wf_rule = WorkflowRule('hive_script')
                wf_rule.custom_action_script = action_id
            elif '.sh' in action_id:
                wf_rule = WorkflowRule('shell_script')
                wf_rule.custom_action_script = action_id
            else:
                err_msg = 'Unsupported action type: {0}'.format(action_id)
                raise ValueError(err_msg)
            rules.append(wf_rule)
        return rules

    def get_custom_rules(self, file_name):
        """Generates workflow rules based on dsl content
        Args:
            file_name: DSL file name. Just the name, not full path
        Returns:
            rules: list of [ibis.inventor.dsl_parser.WorkflowRule]
        """
        rules = []
        if not file_name:
            return rules
        _path = self._get_file_path(file_name)
        action_data = self.parse_file(_path)
        rules = self.generate_rules(action_data)
        return rules
