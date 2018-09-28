"""DSL parser tests"""
import unittest
import os
import difflib
from ibis.utilities.config_manager import ConfigManager
from ibis.settings import UNIT_TEST_ENV
from ibis.inventor.dsl_parser import DSLParser


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class DSLParserTest(unittest.TestCase):
    """Tests the functionality of the DSLParser class"""

    def setUp(self):
        """Setup."""
        self.cfg_mgr = ConfigManager(UNIT_TEST_ENV)
        self.cfg_mgr.requests_dir = BASE_DIR
        self.dsl_parser = DSLParser(
            self.cfg_mgr, ['import_prep', 'import', 'avro'],
            self.cfg_mgr.requests_dir)
        self.cfg_mgr.hadoop_credstore_password_disable = False

    def compare_files(self, test_xml, expected_xml):
        """Compare two xml files."""
        same = True
        test_xml = [xml.strip().replace('\t', '') for xml in
                    test_xml.splitlines()]
        expected_xml = [xml.strip().replace('\t', '') for xml in
                        expected_xml.splitlines()]

        if "".join(expected_xml) != "".join(test_xml):
            same = False
            print ""
            print test_xml
            print expected_xml
            print "XML strings don't match."
            diff = difflib.unified_diff(expected_xml, test_xml)
            print '\n'.join(list(diff))
        return same

    def test_parse_file(self):
        """test parse file"""
        path = self.dsl_parser._get_file_path(
            'test_resources/custom_config.dsl')
        action_data = self.dsl_parser.parse_file(path)
        expected = ['import_prep', 'import', 'avro', '/DEV/ibis2.hql',
                    'test_shell.sh']
        self.assertEquals(action_data, expected)

    def test_parse_file_invalid(self):
        """test parse file invalid"""
        path = self.dsl_parser._get_file_path(
            'test_resources/custom_config_invalid.dsl')
        with self.assertRaises(ValueError) as context:
            _ = self.dsl_parser.parse_file(path)
        self.assertTrue(
            "Fix this line: 'act invalid_action'" in str(context.exception))

    def test_generate_rules(self):
        """test generate_rules"""
        rules = self.dsl_parser.generate_rules(
            ['avro', '/DEV/dir/test.hql', 'action.sh'])
        self.assertEquals(len(rules), 3)
        self.assertEquals(rules[0].action_id, 'avro')
        self.assertEquals(rules[1].action_id, 'hive_script')
        self.assertEquals(rules[1].custom_action_script, '/DEV/dir/test.hql')
        self.assertTrue(rules[1].is_custom_action)
        self.assertTrue(rules[1].is_hive_script)
        self.assertEquals(rules[2].action_id, 'shell_script')
        self.assertEquals(rules[2].custom_action_script, 'action.sh')
        self.assertTrue(rules[2].is_custom_action)
        self.assertFalse(rules[2].is_hive_script)

if __name__ == '__main__':
    unittest.main()
