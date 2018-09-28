"""Generalized Hive table interface. Not specific to any hive table."""
import traceback
from impala.dbapi import connect
from thrift.transport.TTransport import TTransportException
from ibis.custom_logging import get_logger

IMPALA_CONN = None


class Inventory(object):
    """Inventory super class."""

    def __init__(self, cfg_mgr):
        """Init."""
        self.cfg_mgr = cfg_mgr  # ConfigManager object
        self.table = None
        self.logger = get_logger(self.cfg_mgr)

    def _connect(self, host, port, use_kerberos):
        """Use impala to connect to host."""
        global IMPALA_CONN
        try:
            if not IMPALA_CONN:
                IMPALA_CONN = connect(host=host, port=port, timeout=600,
                                      use_kerberos=use_kerberos)
                info_msg = "Connection to the host: {host} successful"
                info_msg = info_msg.format(host=host)
                self.logger.info(info_msg)
            self._cursor = IMPALA_CONN.cursor()
        except (AttributeError, TTransportException):
            err_msg = ("Can't connect to host: {host}, port:"
                       " {port}, error: {error}")
            err_msg = err_msg.format(host=host, port=port,
                                     error=traceback.format_exc())
            self.logger.error(traceback.format_exc())
            raise ValueError(err_msg)

    @classmethod
    def close(cls):
        """Close impala connection."""
        global IMPALA_CONN
        if IMPALA_CONN:
            IMPALA_CONN.close()

    def _execute(self, query):
        """impala cursor.execute"""
        self._cursor.execute(
            query, configuration={'request_pool': self.cfg_mgr.queue_name})

    def get_all(self, where_condition):
        """Return all records matching the where condition.
        Args:
            where_condition(str): where condition in query
        """
        self._connect(self.cfg_mgr.host, self.cfg_mgr.port,
                      self.cfg_mgr.use_kerberos)
        query = "select * from {table_name} where {where_condition}"
        query = query.format(table_name=self.table,
                             where_condition=where_condition)
        self._execute(query)
        results = self._cursor.fetchall()
        return results

    def get_metadata(self):
        """"Return the table metadata/columns required to
        map the table values as a list.
        """
        self._connect(self.cfg_mgr.host, self.cfg_mgr.port,
                      self.cfg_mgr.use_kerberos)
        self._execute('describe ' + self.table)
        meta = self._cursor.fetchall()
        meta_list = []
        for col in meta:
            meta_list.append(col[0])
        if not meta_list:
            err_msg = 'Metadata does not exist for {table}'
            err_msg = err_msg.format(table=self.table)
            self.logger.error(err_msg)
        return meta_list

    def get_rows(self, query):
        """Fetch rows from hive. Used for queries which
           return some data back.
        """
        self._connect(self.cfg_mgr.host, self.cfg_mgr.port,
                      self.cfg_mgr.use_kerberos)
        rows = None
        try:
            self._execute(query)
            rows = self._cursor.fetchall()
        except AttributeError:
            err_msg = ('Connection issue. Please verify host, {host},'
                       ' and kerberos ticket.')
            err_msg = err_msg.format(host=self.cfg_mgr.host)
            self.logger.error('Error found in inventory.get_rows. '
                              'Exit process with errors '
                              '- reason: {0}'.format(err_msg))
            self.logger.error(traceback.format_exc())
        return rows

    def run_query(self, query, table_name=None, refresh=True):
        """Used for running queries which dont return any results. Ex: insert.
        Args:
            query: impala query
            table_name: database.table
            refresh: invalidates table in impala
        """
        self._connect(self.cfg_mgr.host, self.cfg_mgr.port,
                      self.cfg_mgr.use_kerberos)
        try:
            self._execute(query)
            if refresh and table_name:
                _query = 'INVALIDATE METADATA {table}'
                _query = _query.format(table=table_name)
                self._execute(_query)
        except AttributeError:
            err_msg = ('Exit process with errors - reason. Connection issue.'
                       ' Please verify host {host} and kerberos ticket.')
            err_msg = err_msg.format(host=self.cfg_mgr.host)
            self.logger.error(err_msg)
            self.logger.error(traceback.format_exc())
            _err = 'Error: failed running query: {0}'.format(query)
            raise ValueError(_err)

    def get_table_mapping(self, db_name, table_name, db_env):
        """Return a dictionary with table metadata
        mapped to associated values.
        """
        self._connect(self.cfg_mgr.host, self.cfg_mgr.port,
                      self.cfg_mgr.use_kerberos)
        info_msg = 'Creating table mapping for {db_name} {table_name} {db_env}'
        info_msg = info_msg.format(db_name=db_name, table_name=table_name,
                                   db_env=db_env)
        self.logger.info(info_msg)
        meta = self.get_metadata()
        where_condition = ("source_database_name='{0}' and "
                           "source_table_name='{1}' and "
                           "db_env='{2}'")
        where_condition = where_condition.format(db_name, table_name, db_env)
        results_list = self.get_all(where_condition)
        mapping = {}
        if results_list:
            results = results_list[0]
            if not results:
                err_msg = ('Metadata values does not exist'
                           ' for: {db_name} {table_name} {db_env}')
                err_msg = err_msg.format(
                    db_name=db_name, table_name=table_name, db_env=db_env)
                self.logger.warning(err_msg)
            elif len(meta) != len(results):
                err_msg = ('Number of metadata values does not match values'
                           ' for mapping {db_name} {table_name} {db_env}')
                err_msg = err_msg.format(
                    db_name=db_name, table_name=table_name, db_env=db_env)
                self.logger.error(err_msg)
            else:
                # Create a dictionary that maps meta values to results values
                for i in range(0, len(meta)):
                    mapping[meta[i]] = results[i]
        else:
            err_msg = ('Requested meta values for a '
                       'non existent table: {db_name} {table_name} {db_env}')
            err_msg = err_msg.format(db_name=db_name, table_name=table_name,
                                     db_env=db_env)
            self.logger.warning(err_msg)
        return mapping
