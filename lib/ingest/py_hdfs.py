"""Save rows to hive table by manipulating files"""
import subprocess
import os.path
from random import choice
import string
from string import ascii_uppercase

BASE_HDFS_CMD = ['hadoop', 'fs']
HDFS_PUT_CMD = ['-put', '-f']
HDFS_APPEND_CMD = ['-appendToFile']


class PyHDFS(object):

    """Helper class performs Hadoop file system operations"""

    def __init__(self, hdfs_base_dir):
        self.hdfs_base_dir = hdfs_base_dir

    def execute(self, *args):
        """Method executes HDFS command via subprocess.

        :param args: Optional arguments
        """
        hdfs_cmd = []
        hdfs_cmd.extend(BASE_HDFS_CMD)

        for arg in args:
            if arg:
                hdfs_cmd.append(arg)

        proc = subprocess.Popen(hdfs_cmd, stdout=subprocess.PIPE)
        (response, error_msg) = proc.communicate()
        return (response, proc.returncode, error_msg)

    def execute_with_echo(self, filepath, data, is_append=False):
        """Method executes HDFS command via subprocess. It uses
        echo to post the data into stdout to avoid writing the
        data into a file every time.

        :param filepath: Path of HDFS data file
        :param data: Data to be updated/inserted
        :param is_append: Flag determines insert or update data
        """
        hdfs_cmd = []
        hdfs_cmd.extend(BASE_HDFS_CMD)

        if is_append:
            hdfs_cmd.extend(HDFS_APPEND_CMD)
        else:
            hdfs_cmd.extend(HDFS_PUT_CMD)

        with open('row_data.txt', 'wb') as file_h:
            file_h.write(data)

        # source file
        hdfs_cmd.append('row_data.txt')

        # destination file
        hdfs_cmd.append(self.hdfs_base_dir + filepath)
        proc_hdfs = subprocess.Popen(
            hdfs_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            shell=False)
        (response, error_msg) = proc_hdfs.communicate()
        return (response, proc_hdfs.returncode, error_msg)

    def get_file(self, filepath, *options):
        """Returns file name for the given file path"""
        opts_as_str = self.tuple_to_string(options)
        response, return_code, error_msg = self.execute(
            '-ls', opts_as_str, self.hdfs_base_dir + filepath + '/*')
        if not return_code:
            return os.path.basename(response), return_code, error_msg
        else:
            return response, return_code, error_msg

    def insert_update(self, filepath, data, is_append=False):
        """Inserts or updates data into HDFS data file.

        :param filepath: Path of HDFS data file
        :param data: Data to be updated/inserted
        :param is_append: Flag determines insert or update data
        """
        if filepath:
            response, _, _ = self.get_file(filepath)
            filename = response.strip()
        else:
            filename = self.gen_random_filename()

        filepath = filepath + '/' + filename
        return self.execute_with_echo(filepath, data, is_append)

    def tuple_to_string(self, _tuple):
        """Strips commas, single-quotes and parenthesis
        from the given tuple
        """
        to_str = str(_tuple).strip('()')
        to_str = to_str.replace('\'', '')
        to_str = to_str.replace(',', '')
        return to_str

    def gen_random_filename(self):
        """Generates random file name"""
        return 'PBO_' + (''.join(choice(ascii_uppercase +
                                        string.digits) for _ in range(10)))
