"""Convert XML to pdf for visualization"""
import os
import re
from defusedxml.minidom import parseString
import pydot

# List of colours
# http://wingraphviz.sourceforge.net/wingraphviz/language/colorname.htm


class VizOozie(object):
    """VizOozie is an Oozie workflow visualization tool
    Found at https://github.com/iprovalo/vizoozie modified for ibis use"""
    properties = {}

    def __init__(self, cfg_mgr):
        self.cfg_mgr = cfg_mgr

    def loadProperties(self):
        with open(self.cfg_mgr.vizoozie) as f:
            for line in f:
                key, val = line.split('=')
                self.properties[key] = val

    def getName(self, node):
        attr = self.getAttribute(node, "name")
        return attr

    def getTo(self, node):
        attr = self.getAttribute(node, "to")
        return attr

    def getAttribute(self, node, attributeName):
        attr = node.getAttribute(attributeName)
        return attr

    def getOK(self, node):
        ok = node.getElementsByTagName("ok")[0]
        return ok

    def processHeader(self, name):
        output = "digraph{\nsize = \"8,8\";ratio=fill;node" \
                 "[fontsize=24];labelloc=\"t\";label=\"" + name + "\";\n"
        return output

    def processStart(self, doc):
        output = ''
        start = doc.getElementsByTagName("start")[0]
        to = self.getTo(start)
        output = '\n' + "start -> " + to.replace('-', '_') + ";\n"
        return output

    def processAction(self, doc):
        output = ''
        for node in doc.getElementsByTagName("action"):
            name = self.getName(node)
            color = "white"
            for key, value in self.properties.iteritems():
                if len(node.getElementsByTagName(key)) != 0:
                    color = value
                    break
            ok = self.getOK(node)
            to = self.getTo(ok)
            output += '\n' + name.replace('-',
                                          '_') + " " \
                                                 "[shape=box,style=" \
                                                 "filled,color=" + \
                      color + "];\n"
            output += '\n' + name.replace('-', '_') + " -> " + \
                to.replace('-', '_') + ";\n"
        return output

    def processFork(self, doc):
        output = ''
        for node in doc.getElementsByTagName("fork"):
            name = self.getName(node)
            output += '\n' + name.replace('-', '_') + " [shape=octagon];\n"
            for path in node.getElementsByTagName("path"):
                start = path.getAttribute("start")
                output += '\n' + name.replace(
                    '-', '_') + " -> " + start.replace('-', '_') + ";\n"
        return output

    def processJoin(self, doc):
        output = ''
        for node in doc.getElementsByTagName("join"):
            name = self.getName(node)
            to = self.getTo(node)
            output += '\n' + name.replace('-', '_') + " [shape=octagon];\n"
            output += '\n' + name.replace('-', '_') + " -> " + to.replace(
                '-', '_') + ";\n"
        return output

    def processKey(self):
        hive = "darkgoldenrod1"
        shell = "seagreen3"
        sqoop = "lightcoral"
        output = ''
        output += '\n' + "Sqoop" + " [shape=box,style=filled,color=" + \
                  sqoop + "];\n"
        output += "Shell" + " [shape=box,style=filled,color=" + shell + "];\n"
        output += "Hive" + " [shape=box,style=filled,color=" + hive + "];\n"
        return output

    def processDecision(self, doc):
        output = ''
        for node in doc.getElementsByTagName("decision"):
            name = self.getName(node)
            switch = node.getElementsByTagName("switch")[0]
            output += '\n' + name.replace('-', '_') + " [shape=diamond];\n"
            for case in switch.getElementsByTagName("case"):
                to = case.getAttribute("to")
                caseValue = case.childNodes[0].nodeValue.replace('"', '')
                output += '\n' + name.replace('-', '_') + " -> " + \
                    to.replace('-', '_') + "[style=bold,label=\"" + \
                    caseValue + "\",fontsize=20];\n"

            default = switch.getElementsByTagName("default")[0]
            to = default.getAttribute("to")
            output += '\n' + name.replace('-', '_') + " -> " \
                      + to.replace('-', '_') \
                      + "[style=dotted,label=\"default\",fontsize=20];\n"
        return output

    def processCloseTag(self):
        output = '\n' + "}"
        return output

    def convertWorkflowXMLToDOT(self, input_str, name):
        self.loadProperties()
        pat = re.compile(r'<arg>.*(["><\'\&]).*</arg>')
        re_search = re.search(pat, input_str)
        if re_search and len(re_search.groups()) >= 1:
            input_str = re.sub(pat, 'query', input_str)
        doc = parseString(input_str)
        output = self.processHeader(name)
        output += self.processStart(doc)
        output += self.processAction(doc)
        output += self.processFork(doc)
        output += self.processJoin(doc)
        output += self.processDecision(doc)
        output += self.processKey()
        output += self.processCloseTag()
        return output

    def convertDotToPDF(self, dot_string, name):
        """Writes the dot string to a .dot file than creates
        a pdf from the dot file"""
        dot = name + '.dot'
        dot = os.path.join(self.cfg_mgr.files, dot)
        dot_output = open(dot, 'w')
        dot_output.write(str(dot_string))
        dot_output.close()
        png = os.path.join(self.cfg_mgr.files, name + '.pdf')
        # Write out the pdf file - note that there are many other options
        graph = pydot.graph_from_dot_file(dot)
        graph.write(png, format='pdf')

    def visualizeXML(self, wf_name):
        """Given an xml workflow, generates a visual of the workflow in a pdf
        Args:
            wf_name: file name
        """
        wf_path = os.path.join(self.cfg_mgr.files, '{0}.xml'.format(
            wf_name))
        xml_file = open(wf_path, 'r')
        input_str = xml_file.read()
        output = self.convertWorkflowXMLToDOT(input_str,
                                              os.path.basename(xml_file.name))
        self.convertDotToPDF(output, os.path.basename(
            os.path.splitext(xml_file.name)[0]))
        xml_file.close()
