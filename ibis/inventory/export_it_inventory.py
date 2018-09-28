"""Export IT inventory."""
from ibis.inventory.inventory import Inventory
from ibis.model.exporttable import ItTableExport


class ExportITInventory(Inventory):
    """Class used for managing the records of source table connections.
    and properties in the it_table.
    """

    def __init__(self, *arg):
        """Init."""
        super(self.__class__, self).__init__(*arg)
        self.table = self.cfg_mgr.it_table_export

    def insert_export(self, it_table):
        """Insert a dictionary representation of a table
           into the it table export."""
        insert = False
        tbl_connection_factory = it_table.connection_factories
        where_condition = ("source_database_name='{0}' and "
                           "source_table_name='{1}' and "
                           "db_env='{2}'")
        where_condition = where_condition.format(
            it_table.database, it_table.table_name, it_table.db_env)

        if not self.get_all(where_condition):
            values = ("'{full_table_name}', '{source_dir}',"
                      "{mappers}, '{jdbcurl}',"
                      "'{connection_factories}', '{db_username}',"
                      "'{password_file}', '{frequency}', {fetch_size},"
                      "'{target_schema}', '{target_table}',"
                      "'{staging_database}'")
            values = values.format(source_dir=it_table.dir,
                                   jdbcurl=it_table.jdbcurl,
                                   db_username=it_table.username,
                                   password_file=it_table.password_file,
                                   target_schema=it_table.target_schema,
                                   target_table=it_table.target_table,
                                   full_table_name=it_table.full_name,
                                   mappers=it_table.mappers,
                                   connection_factories=tbl_connection_factory,
                                   frequency=it_table.weight,
                                   fetch_size=it_table.fetch_size,
                                   staging_database=it_table.staging_database)
            query = ("insert into table {it_tbl_export} partition "
                     "(source_database_name='{db}', source_table_name"
                     "='{tbl}', db_env='{db_env}') values ({values})")
            query = query.format(it_tbl_export=self.table,
                                 db=it_table.database,
                                 tbl=it_table.table_name,
                                 db_env=it_table.db_env,
                                 values=values)
            self.run_query(query, self.table)
            insert = True
            msg = ('Insert new record {db} {table} {db_env} into '
                   ' {it_table_export}')
            msg = msg.format(db=it_table.database, table=it_table.table_name,
                             db_env=it_table.db_env,
                             it_table_export=self.table)
            self.logger.info(msg)
        else:
            msg = ('Table {db} {tbl} {db_env} NOT INSERTED into '
                   ' {it_table_export} Already exists!')
            msg = msg.format(db=it_table.database, tbl=it_table.table_name,
                             db_env=it_table.db_env,
                             it_table_export=self.table)
            self.logger.warning(msg)
        return insert, msg

    def update_export(self, it_table):
        """Updates/overwrites a record with a provided
        dictionary representation of a table
        """
        updated = False
        tbl_connection_factory = it_table.connection_factories
        where_condition = ("source_database_name='{0}' and "
                           "source_table_name='{1}' and "
                           "db_env='{2}'")
        where_condition = where_condition.format(
            it_table.database, it_table.table_name, it_table.db_env)

        if self.get_all(where_condition):
            values = ("'{full_table_name}', '{source_dir}',"
                      "{mappers}, '{jdbcurl}',"
                      "'{connection_factories}', '{db_username}',"
                      "'{password_file}', '{frequency}', {fetch_size},"
                      "'{target_schema}', '{target_table}',"
                      "'{staging_database}'")
            values = values.format(source_dir=it_table.dir,
                                   jdbcurl=it_table.jdbcurl,
                                   db_username=it_table.username,
                                   password_file=it_table.password_file,
                                   target_schema=it_table.target_schema,
                                   target_table=it_table.target_table,
                                   full_table_name=it_table.full_name,
                                   mappers=it_table.mappers,
                                   connection_factories=tbl_connection_factory,
                                   frequency=it_table.weight,
                                   fetch_size=it_table.fetch_size,
                                   staging_database=it_table.staging_database)
            query = ("insert overwrite table {it_tbl_export} partition "
                     "(source_database_name='{db}', source_table_name"
                     "='{tbl}', db_env='{db_env}') values ({values})")
            query = query.format(it_tbl_export=self.table,
                                 db=it_table.database,
                                 tbl=it_table.table_name,
                                 db_env=it_table.db_env,
                                 values=values)
            self.run_query(query, self.table)
            updated = True
            msg = 'Updated table {db} {table} {db_env} in {it_table_export}'
            msg = msg.format(db=it_table.database, table=it_table.table_name,
                             db_env=it_table.db_env,
                             it_table_export=self.table)
            self.logger.info(msg)
        else:
            msg = ('Table {db} {tbl} {db_env} NOT UPDATED in {it_table_export}'
                   ' It does not exist!')
            msg = msg.format(db=it_table.database, tbl=it_table.table_name,
                             db_env=it_table.db_env,
                             it_table_export=self.table)
            self.logger.warning(msg)
        return updated, msg

    def parse_requests_export(self, requests):
        """Return a list of dictionaries of table representation.
        Args:
            requests: List of ibis.inventory.request_inventory.Request
        Returns:
            List[{ibis.model.exporttable.ItTableExport}].
        """
        tables = []
        for request_table in requests:
            it_table = ItTableExport(request_table.get_meta_data(),
                                     self.cfg_mgr)
            tables.append(it_table)
        return tables

    def get_all_tables_export(self):
        """Return a list of all records from it_table_export."""
        tables = []
        query = 'SELECT * FROM {tbl}'.format(tbl=self.table)
        result = self.get_rows(query)
        if result:
            for table_row in result:
                tables.append(self._build_row_dict_export(table_row))
        return tables

    def _build_row_dict_export(self, row):
        """Builds it_table row dict
        Args:
            row: list of row values from it_table
        """
        tbl_dict = {
            'full_table_name': row[0], 'source_dir': row[1], 'mappers': row[2],
            'jdbcurl': row[3], 'connection_factories': row[4],
            'db_username': row[5], 'password_file': row[6],
            'frequency': row[7], 'fetch_size': row[8], 'target_schema': row[9],
            'target_table': row[10], 'staging_database': row[11],
            'source_database_name': row[-3], 'source_table_name': row[-2],
            'db_env': row[-1]}
        return tbl_dict
