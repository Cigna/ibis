"""Oozie API handlers"""
import json
import os
import requests
from requests_kerberos import HTTPKerberosAuth
from ibis.custom_logging import get_logger


class OozieAPi(object):
    """handles oozie api"""

    def __init__(self, cfg_mgr):
        """init"""
        self.oozie_url = cfg_mgr.oozie_url
        self.cfg_mgr = cfg_mgr
        self.logger = get_logger(self.cfg_mgr)

    def put_request(self, suffix_url, xml_file):
        """Make a oozie put request"""
        status = False
        file_h = open(os.path.join(self.cfg_mgr.saves, xml_file))
        data = file_h.read()
        http_headers = {'Content-Type': 'application/xml'}
        url = self.oozie_url + suffix_url
        self.logger.info(url)
        response = requests.post(url, data=data, headers=http_headers,
                                 auth=HTTPKerberosAuth())
        if 200 <= response.status_code and response.status_code < 300:
            my_json = json.dumps(response.json())
            res_val = response.json()
            # self.logger.info(json.dumps(response.json(),
            # indent=4, sort_keys=True))
            msg = "\n\n\t\tGo here: https://{0}:8888/oozie/" \
                  "list_oozie_workflow/{1}/\n".\
                format(self.cfg_mgr.workflow_host, res_val['id'])
            self.logger.info(msg)
            status = True
        else:
            self.logger.error("Error submitting job. Error code: "
                              "{status}".format(status=response.status_code))
            self.logger.error(response.text)
        return status

    def start_job(self, xml_file):
        """Runs oozie job via http api"""
        return self.put_request("jobs?action=start", xml_file)
