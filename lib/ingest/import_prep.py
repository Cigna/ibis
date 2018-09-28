"""cache table record"""

import sys
import socket
from itertools import izip
from impala_utils import ImpalaConnect


class CacheTable(object):
    """Source table representation."""

    def __init__(self, database, table_name, db_env, it_table, it_table_host):
        """Init."""
        self.database = database
        self.table_name = table_name
        self.db_env = db_env
        self.it_table = it_table
        self.it_table_host = it_table_host

    def get_record(self):
        """Get it-table record"""
        query = ("SELECT * FROM {0} WHERE "
                 "source_table_name='{1}' AND "
                 "source_database_name='{2}' AND "
                 "db_env='{3}';")
        query = query.format(self.it_table, self.table_name,
                             self.database, self.db_env)
        row = None
        try:
            row = ImpalaConnect.run_query(self.it_table_host, self.it_table,
                                          query, op='select')
        except socket.error as err:
            print "Socket error({0}): {1}".format(err.errno, err.strerror)
            print 'Trying again...'
            row = ImpalaConnect.run_query(self.it_table_host, self.it_table,
                                          query, op='select')

        if not row:
            err_msg = "Failed to fetch results for query: {0}"
            err_msg = err_msg.format(query)
            raise ValueError(err_msg)

        return row[0]

    def get_schema(self):
        """Get it-table schema"""
        query = "describe {0};".format(self.it_table)
        row = ImpalaConnect.run_query(self.it_table_host, self.it_table,
                                      query, op='select')
        if not row:
            err_msg = "Failed to fetch results for query: {0}"
            err_msg = err_msg.format(query)
            raise ValueError(err_msg)
        return row

    def create_env_file(self):
        """Creates shell env vars equivalent of it-table row"""
        env_template = "export {0}='{1}'\n"
        table_props = self.get_record()
        schema_record = self.get_schema()

        with open('it_table_env.sh', 'wb') as fileh:
            perf_env = 'NOT_PERF'
            if 'perf_it_table' in self.it_table:
                perf_env = 'PERF'
            line = env_template.format('PERF_ENV', perf_env)
            fileh.write(line)

            for schema, column_value in izip(schema_record, table_props):
                column_name = schema[0]
                line = env_template.format(column_name, column_value)
                fileh.write(line)


if __name__ == '__main__':
    if len(sys.argv) != 6:
        print "ERROR----> command line args are not proper"
        sys.exit(1)

    params = {
        'database': sys.argv[1],
        'table_name': sys.argv[2],
        'db_env': sys.argv[3],
        'it_table': sys.argv[4],
        'it_table_host': sys.argv[5]
    }

    cache_obj = CacheTable(params['database'], params['table_name'],
                           params['db_env'],
                           params['it_table'], params['it_table_host'])
    cache_obj.create_env_file()
