"""Used for creating merge requests in GitLab projects"""
import json
import requests
from ibis.custom_logging import get_logger


class GitlabAPI(object):
    """Handles gitlab api"""

    SSL_VERIFY = True

    def __init__(self, cfg_mgr):
        """init"""
        self.api_url = 'http://fake.git/api/v3'
        self.cfg_mgr = cfg_mgr
        self.logger = get_logger(self.cfg_mgr)
        # gitlab user: fake_username's private token
        self.private_token = ''

    def put_request(self, suffix_url, data):
        """Make a merge request"""
        status = False
        res_val = None
        url = self.api_url + suffix_url
        http_headers = {'PRIVATE-TOKEN': self.private_token}
        response = requests.post(url, data=data, headers=http_headers,
                                 verify=self.SSL_VERIFY)

        if response.status_code >= 200 and response.status_code < 300:
            my_json = json.dumps(response.json())
            res_val = response.json()
            status = True
        else:
            self.logger.error("Error submitting job. "
                              "Error code: {status}".
                              format(status=response.status_code))
            self.logger.error(response.text)
        return status, res_val

    def get_request(self, suffix_url):
        """Get projects"""
        status = False
        res_val = None
        http_headers = {'PRIVATE-TOKEN': self.private_token}
        url = self.api_url + suffix_url
        # 'users?username=M77773'
        response = requests.get(url, headers=http_headers,
                                verify=self.SSL_VERIFY)

        if response.status_code >= 200 and response.status_code < 300:
            my_json = json.dumps(response.json())
            res_val = response.json()
            status = True
        else:
            err_msg = "Error submitting job. Error code: {status}"
            err_msg = err_msg.format(status=response.status_code)
            self.logger.error(err_msg)
            self.logger.error(response.text)
        return status, res_val

    def get_open_merge_requests(self):
        """fetch all open merge requests"""
        suffix_url = '/projects/1638/merge_requests?state=opened'
        _, ret = self.get_request(suffix_url)
        if ret:
            return ret

    def get_merge_request_url(self, merge_request):
        """Build web url for merge request
        Project ID for ibis-workflows gitlab is: 1638
        """
        web_url = ('http://fake.git/fake_teamname'
                   '/ibis-workflows/merge_requests/{iid}/diffs')
        suffix_url = '/projects/1638/merge_requests/{0}/changes'
        suffix_url = suffix_url.format(merge_request['id'])
        _, ret = self.get_request(suffix_url)
        if ret:
            web_url = web_url.format(iid=ret['iid'])
            return web_url

    def make_workflows_merge_request(self, branch_name):
        """creates a merge request with master for ibis-workflows repo
        1683 is the project id of ibis-workflows repo
        942 is the user id for shantanu
        """
        already_open = False
        curr_merge_req = {}

        open_merges = self.get_open_merge_requests()

        if open_merges is not None:
            for merge_req in self.get_open_merge_requests():
                if merge_req['source_branch'] == branch_name:
                    already_open = True
                    curr_merge_req = merge_req
                    break

        suffix_url = '/projects/1638/merge_requests'
        title = 'Auto-ibis-merge-request: ' + branch_name
        web_url = ''
        data = {
            'id': 1638,
            'source_branch': branch_name,
            'target_branch': self.cfg_mgr.git_workflows_dir.lower(),
            'assignee_id': '942',
            'title': title,
            'remove_source_branch': True
        }

        if not already_open:
            _, curr_merge_req = self.put_request(suffix_url, data)

        if curr_merge_req:
            web_url = self.get_merge_request_url(curr_merge_req)
            self.logger.info('Merge request succesful! Merge request '
                             'title: {0}'.format(title))
            return True, title, web_url
        else:
            return False, title, web_url


if __name__ == '__main__':
    obj = GitlabAPI('')
    obj.get_request()
