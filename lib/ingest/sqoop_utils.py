"""Sqoop eval helper"""
import os
import subprocess
import re


class SqoopUtils(object):
    """Sqoop helper"""

    def __init__(self, sqoop_jars, jdbc_url, connection_factories, user_name,
                 jceks, password):
        """init
        Args:
            jceks: either jceks hdfs file or None(string) when no jceks
            password: either password-file or password-alias
        """
        self.jars = sqoop_jars
        self.jdbc_url = jdbc_url
        self.user_name = user_name
        self.jceks = jceks
        self.password = password
        self.connection_factories = connection_factories

    def eval(self, query):
        """Sqoop relational db data for a query."""
        if self.jceks == 'None':
            sqoop_params = ["sqoop", "eval",
                            "-libjars", self.jars,
                            "--driver", self.connection_factories, "--verbose",
                            "--connect", self.jdbc_url,
                            "--query", query, "--username", self.user_name,
                            "--password-file", self.password]
        else:
            os.environ['HADOOP_CREDSTORE_PASSWORD'] = "none"
            sqoop_params = ["sqoop", "eval",
                            "-D hadoop.security.credential."
                            "provider.path=" + self.jceks,
                            "-libjars", self.jars,
                            "--driver", self.connection_factories, "--verbose",
                            "--connect", self.jdbc_url,
                            "--query", query, "--username", self.user_name,
                            "--password-alias", self.password]
        proc = subprocess.Popen(sqoop_params, stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)

        output, err = proc.communicate()
        return proc.returncode, output, err

    def _clean_query_result(self, output):
        """Match characters between two | characters. Ex: | anything |."""
        pattern = re.compile('\|.*\|')
        return pattern.findall(output)

    def fetch_rows_sqoop(self, output, strip_col_val=True,
                         filter_columns=None):
        """Clean up table output received through sqoop.
        Args:
            output: Sqoop output for query
            strip_col_val: whether to strip spaces around column data
            filter_columns: list of column indexes.
                            Only those column data will be returned and
                            rest all columns will be skipped
        Returns:
            column labels and row data
        """
        raw_data = self._clean_query_result(output)
        row_data = []
        for line in raw_data:
            column_data = line.split('|')
            # ignore the first and last items due to split
            column_data = column_data[1:-1]
            row_data.append(column_data)

        # first item is labels in table output
        column_labels = row_data[0]
        column_labels = [label.strip() for label in column_labels]
        num_column_labels = len(column_labels)
        # row data starts from second item in table output
        row_values = row_data[1:]
        # remove the extra space in the start and end of string
        # which is inserted by sqoop output table
        row_values = [[col_val[1:-1] for col_val in row] for row in row_values]

        if filter_columns:
            pop_columns = []
            for i in range(num_column_labels):
                if i not in filter_columns:
                    pop_columns.append(i)
            pop_columns.sort(reverse=True)
            for index in pop_columns:
                column_labels.pop(index)
                temp_row_values = []
                for row in row_values:
                    row.pop(index)
                    temp_row_values.append(row)
                row_values = temp_row_values

        if strip_col_val:
            row_values = [[val.strip() for val in row] for row in row_values]

        return column_labels, row_values
