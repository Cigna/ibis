"""
Unit tests for Vizoozie. Note that a majority of these tests were taken
from the original Github project https://github.com/iprovalo
"""
import os
import unittest
from defusedxml.minidom import parseString
from ibis.utilities.vizoozie import VizOozie
from ibis.utilities.config_manager import ConfigManager
from ibis.settings import UNIT_TEST_ENV

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class VizOozieTest(unittest.TestCase):
    expected_complete = ('digraph{\nsize = "8,8";ratio=fill;node[fontsize=24]'
                         ';labelloc="t";label="test";\n\nstart -> is_capture'
                         ';\n\ncapture [shape=box,style=filled,color=white\n]'
                         ';\n\ncapture -> complex_math;\n\ncomplex_math '
                         '[shape=box,style=filled,color=white\n];\n'
                         '\ncomplex_math -> end;\n\nis_capture '
                         '[shape=diamond];\n\nis_capture -> '
                         'capture[style=bold,label="${wf:conf'
                         '(capture) == true}",fontsize=20];'
                         '\n\nis_capture -> complex_math['
                         'style=dotted,label="default",f'
                         'ontsize=20];\n\nSqoop [shape=box,'
                         'style=filled,color=lightcoral];\n'
                         'Shell [shape=box,style=filled,'
                         'color=seagreen3];\nHive '
                         '[shape=box,style=filled,color=darkgoldenrod1];\n\n}')

    provided_complete = ('<workflow-app xmlns=\"uri:oozie:workflow:0.2\"'
                         ' name=\"etl\">\n<start to=\"is_capture\" />'
                         '\n<decision name=\"is_capture\">\n<switch>\n <case'
                         ' to=\"capture\">${wf:conf(\"capture\") =='
                         ' \"true\"}</case>\n<default to=\"complex-math\" />'
                         '\n</switch>\n  </decision>\n  <action name='
                         '\"capture\">\n    <pig>    </pig>\n    <ok to='
                         '\"complex-math\" />\n    <error to=\"fail\" />'
                         '\n  </action>\n <action name=\"complex-math\">'
                         '\n    <pig>    </pig>\n    <ok to=\"end\" />\n'
                         '    <error to=\"fail\" /></action>\n  <kill name'
                         '=\"fail\">   <message>Some error message</message>'
                         ' </kill>\n<end name=\"end\" />\n</workflow-app>\n')

    expected_action = ('\ncapture [shape=box,style=filled,color='
                       'white\n];\n\ncapture -> is_cleanse;\n')

    provided_action = (' <action name=\"capture\"> <pig>    </pig> <ok to='
                       '\"is_cleanse\" /> <error to=\"fail\" /> </action>')

    expected_fork = ('\npost_process [shape=octagon];\n\npost_process ->'
                     ' complex_math;\n\npost_process -> more_complex;\n\n'
                     'post_process -> geek_candy_process;\n')
    provided_fork = ('  <fork name=\"post-process\">  <path start'
                     '=\"complex-math\" />  <path start=\"more-complex\" />'
                     '  <path start=\"geek-candy-process\" /></fork>')

    expected_join = ('\njoin_post_process [shape=octagon];'
                     '\n\njoin_post_process -> end;\n')
    provided_join = '  <join name=\"join-post-process\" to=\"end\" />'

    expected_decision = ('\nis_cleanse [shape=diamond];\n\nis_cleanse ->'
                         ' cleanse[style=bold,label="${wf:conf(cleanse) '
                         '== true}",fontsize=20];\n\nis_cleanse -> '
                         'end[style=dotted,label="default",fontsize=20];\n')

    provided_decision = ('<decision name=\"is_cleanse\"><switch><case '
                         'to=\"cleanse\">${wf:conf(\"cleanse\") == \"true\"}'
                         '</case><default to=\"end\" /></switch></decision>')

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.vizoozie = VizOozie(ConfigManager(UNIT_TEST_ENV))

    def test_e2e(self):
        result = self.vizoozie.convertWorkflowXMLToDOT(self.provided_complete,
                                                       'test')
        self.assertEqual(result, self.expected_complete)

    def test_action(self):
        self.vizoozie.loadProperties()
        doc = parseString(self.provided_action)
        result = self.vizoozie.processAction(doc)
        self.assertEqual(result, self.expected_action)

    def test_fork(self):
        self.vizoozie.loadProperties()
        doc = parseString(self.provided_fork)
        result = self.vizoozie.processFork(doc)
        self.assertEqual(result, self.expected_fork)

    def test_join(self):
        self.vizoozie.loadProperties()
        doc = parseString(self.provided_join)
        result = self.vizoozie.processJoin(doc)
        self.assertEqual(result, self.expected_join)

    def test_decision(self):
        self.vizoozie.loadProperties()
        doc = parseString(self.provided_decision)
        result = self.vizoozie.processDecision(doc)
        self.assertEqual(result, self.expected_decision)


if __name__ == '__main__':
    unittest.main()
