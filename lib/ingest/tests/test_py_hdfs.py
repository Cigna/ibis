import unittest
from mock import patch
from lib.ingest.py_hdfs import PyHDFS

BASE_DIR = '/user/dev/data/checks_balances/'


class PopenClose(object):
    def close(self):
        pass


class PopenOut(object):
    def __init__(self, std_out):
        self.stdout = std_out
        self.returncode = 0

    def communicate(self):
        return (self.stdout, 'No Error')


class PopenOne(object):
    def __init__(self, std_out):
        self.stdout = PopenClose()
        self.stdin = std_out
        self.returncode = 0

    def communicate(self):
        return (self.stdout, 'No Error')


class PyHDFSTest(unittest.TestCase):
    """Test PyHDFS class"""

    def setUp(self):
        self.pyhdfs = PyHDFS(BASE_DIR)

    def tearDown(self):
        self.pyhdfs = None

    @patch('lib.ingest.py_hdfs.PyHDFS.execute',
           return_value=('Ok Response', 0, None))
    def test_get_file(self, mock_execute):
        res = self.pyhdfs.get_file('/user', )
        self.assertEqual(res[0], 'Ok Response')

    @patch('lib.ingest.py_hdfs.PyHDFS.execute_with_echo',
           return_value=('Execute Response', 0, None))
    @patch('lib.ingest.py_hdfs.PyHDFS.execute',
           return_value=('Get File Response', 0, None))
    def test_insert_update(self, mock_execute_with_echo, mock_execute):
        res = self.pyhdfs.insert_update('/user', 'sample data')
        self.assertEqual(res[0], 'Execute Response')

    def test_tuple_to_string(self):
        tuple_val = ('ls', '-R')
        res = self.pyhdfs.tuple_to_string(tuple_val)
        self.assertEqual(res, 'ls -R')

    @patch(
        'subprocess.Popen',
        return_value=PopenOut('2017-03-28 07:41 /user/data/checks_balances/'))
    def test_execute(self, mock_subproc_popen):
        result = self.pyhdfs.execute('-ls')
        expected = '2017-03-28 07:41 /user/data/checks_balances/'
        self.assertEquals(result, (expected, 0, 'No Error'))

    @patch(
        'subprocess.Popen',
        return_value=PopenOne(''), autospec=True)
    @patch(
        'subprocess.Popen',
        return_value=PopenOut(
            ['2017-03-28 07:41 /user/data/checks_balances/']))
    def test_execute_with_echo(self, mock_popen_close, mock_popen):
        result = self.pyhdfs.execute_with_echo('-ls', '2017-03-28 07:41')
        self.assertEquals(0, result[1])

if __name__ == "__main__":
    unittest.main()
