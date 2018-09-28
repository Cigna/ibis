"""Impala utilities for querying"""

import sys
import os
from impala.dbapi import connect


IMPALA_CONN = None
HIVE_CONN = None


class ImpalaConnect(object):
    """Runs query on impala"""

    @classmethod
    def run_query(cls, host_name, table, query, op='select'):
        """Execute a impala query.
        Args:
            host_name: host name for impala connection
            table: database.table
            query: impala query
            op: indicator for select"""
        global IMPALA_CONN

        result = ''
        if not IMPALA_CONN:
            IMPALA_CONN = connect(host=host_name, port=25003, timeout=600,
                                  use_kerberos=True)
        cur = IMPALA_CONN.cursor()
        # Can't invalidate something that doesn't yet exist
        if op != "create":
            ImpalaConnect.invalidate_metadata(host_name, table)
        cur.execute(query, configuration={'request_pool': 'ingestion'})

        if cur:
            if op == 'select':
                result = cur.fetchall()
        else:
            raise "Hive connection - cursor is none"
        return result

    @classmethod
    def close_conn(cls):
        """Close impala connection. impyla lib issue
        - close conn at the end of script.
        """
        global IMPALA_CONN
        if IMPALA_CONN:
            IMPALA_CONN.close()

    @classmethod
    def invalidate_metadata(cls, host_name, table):
        """Execute INVALIDATE METADATA for tables created
        through the Hive shell to be available for Impala queries.
        Args:
            host_name: host name for impala connection
            table: database.table
        """
        global IMPALA_CONN

        if not IMPALA_CONN:
            IMPALA_CONN = connect(host=host_name, port=25003, timeout=600,
                                  use_kerberos=True)
        cur = IMPALA_CONN.cursor()
        cur.execute('invalidate metadata ' + table + ';',
                    configuration={'request_pool': 'ingestion'})

        if not cur:
            raise "Hive connection - cursor is none"


class HiveConnect(object):
    """Runs query on hive"""

    @classmethod
    def run_query(cls, host_name, table, query, op='select'):
        """Execute a hive query.
        Args:
            host_name: host name for hive connection
            table: database.table
            query: hive query
            op: indicator for select"""
        global HIVE_CONN

        result = ''
        if not HIVE_CONN:
            HIVE_CONN = connect(host=host_name, port=25006, timeout=600,
                                use_kerberos=True,
                                kerberos_service_name='hive')
        cur = HIVE_CONN.cursor()
        cur.execute(
            query, configuration={'mapred.job.queue.name': 'ingestion'})

        if cur:
            if op == 'select':
                result = cur.fetchall()
        else:
            raise "Hive connection - cursor is none"
        return result

    @classmethod
    def close_conn(cls):
        """Close hive connection. impyla lib issue -
        close conn at the end of script.
        """
        global HIVE_CONN
        if HIVE_CONN:
            HIVE_CONN.close()


if __name__ == '__main__':
    table = sys.argv[1]
    query = sys.argv[2]
    op = sys.argv[3]
    server = sys.argv[4]

    impala_host = os.environ['IMPALA_HOST']
    hive_host = os.environ['hive2_host']

    if server == 'impala':
        ImpalaConnect.run_query(impala_host, table, query, op=op)
        ImpalaConnect.close_conn()
    elif server == 'hive':
        HiveConnect.run_query(hive_host, table, query, op=op)
        HiveConnect.close_conn()
    else:
        raise ValueError('unknown server name')
