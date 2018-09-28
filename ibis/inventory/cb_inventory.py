"""Checks and balances module."""
from ibis.inventory.inventory import Inventory

req_keys = ['directory', 'pull_time', 'avro_size', 'ingest_timestamp',
            'parquet_time', 'parquet_size', 'rows', 'lifespan', 'ack',
            'cleaned', 'current_repull', 'domain', 'table']
# table* is a combination of db name + '_' + table name


class CheckBalancesInventory(Inventory):
    """Class used for managing the Check Balances table."""

    def __init__(self, *arg):
        """Init."""
        super(self.__class__, self).__init__(*arg)
        self.table = self.cfg_mgr.checks_balances
        self.req_keys = req_keys

    def get(self, db_name, table_name):
        """Return a list of records matching the db_name and table_name."""
        name = db_name + '_' + table_name
        query = 'select * from {table} where `table`=\'{tbl_name}\''
        query = query.format(table=self.table, tbl_name=name)
        self.logger.info("Query executed successfully")
        return self.get_rows(query)

    def update(self, checks_balances):
        """Update a record in the checks and balances table.

        with the provided Checks Balances dictionary
        """
        values = '\'{directory}\', {pull_time}, {avro_size}, ' \
                 '\'{ingest_timestamp}\', {parquet_time}, ' \
                 '{parquet_size}, {rows}, \'{lifespan}\', {ack}, ' \
                 '{cleaned}, {current_repull}'. \
            format(directory=checks_balances['directory'],
                   pull_time=checks_balances['pull_time'],
                   avro_size=checks_balances['avro_size'],
                   ingest_timestamp=checks_balances['ingest_timestamp'],
                   parquet_time=checks_balances['parquet_time'],
                   parquet_size=checks_balances['parquet_size'],
                   rows=checks_balances['rows'],
                   lifespan=checks_balances['lifespan'],
                   ack=checks_balances['ack'],
                   cleaned=checks_balances['cleaned'],
                   current_repull=checks_balances['current_repull'])
        query = ("insert overwrite table {cb_tbl} partition "
                 "(domain='{domain}', `table`='{tbl}') values ({values})")
        query = query.format(
            cb_tbl=self.table, domain=checks_balances['domain'],
            tbl=checks_balances['table'], values=values)
        self.run_query(query, self.cfg_mgr.checks_balances)
        msg = 'Updated table, {tbl}, in Checks and Balances table '.format(
            tbl=checks_balances['table'])
        self.logger.info(msg)
        return msg

    def get_rows_count(self, table_name):
        """Return the last row count of one table."""
        query = "SELECT * FROM {table} WHERE `table` = '{tbl_name}'" \
                " ORDER BY ingest_timestamp DESC"
        query = query.format(table=self.table, tbl_name=table_name)
        tbls = self.get_rows(query)
        if tbls:
            tbl = tbls[0]
            return tbl[6]
        return 0
