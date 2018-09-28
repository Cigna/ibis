"""
DB operations for ibis it_table(dev_it_table, int_it_table, prod_it_table)
"""
import os
from ibis.inventory.inventory import Inventory
from ibis.model.table import ItTable


class ITInventory(Inventory):
    """Class used for managing the records of source table connections.
    and properties in the it_table.
    """

    def __init__(self, *arg):
        """Init."""
        super(self.__class__, self).__init__(*arg)
        self.table = self.cfg_mgr.it_table

    def _build_values_clause(self, row):
        """Values clause for impala query
        Args:
            row: row of it_table. Instance of ibis.model.table.ItTable
        """
        values = ("'{full_table_name}', '{domain}', '{target_dir}',"
                  " '{split_by}', {mappers}, '{jdbcurl}',"
                  " '{connection_factories}', '{db_username}',"
                  " '{password_file}', '{load}', {fetch_size}, {hold},"
                  " '{appl_id}', '{views}', '{esp_group}',"
                  " '{check_column}', '{source_schema_name}',"
                  " '{sql_query}', '{actions}'")
        values = values.format(
            full_table_name=row.full_name, domain=row.domain,
            target_dir=row.target_dir, split_by=row.split_by,
            mappers=row.mappers, jdbcurl=row.jdbcurl,
            connection_factories=row.connection_factories,
            db_username=row.username, password_file=row.password_file,
            load=row.frequency_load, fetch_size=row.fetch_size, hold=row.hold,
            appl_id=row.esp_appl_id, views=row.views, esp_group=row.esp_group,
            check_column=row.check_column, source_schema_name=row.schema,
            sql_query=row.query, actions=row.actions)
        return values

    def insert(self, it_table):
        """Insert a dictionary representation of a table into the it table.
        Args:
            it_table: instance of ibis.model.table.ItTable. it_table row
        """
        success = False
        where_condition = ("source_database_name='{0}' and "
                           "source_table_name='{1}' and "
                           "db_env='{2}'")
        where_condition = where_condition.format(
            it_table.database, it_table.table_name, it_table.db_env)

        if not self.get_all(where_condition):
            values = self._build_values_clause(it_table)
            query = ("INSERT INTO TABLE {it_tbl} PARTITION "
                     "(source_database_name='{db}', source_table_name"
                     "='{tbl}', db_env='{db_env}') VALUES ({values})")
            query = query.format(it_tbl=self.table, db=it_table.database,
                                 tbl=it_table.table_name,
                                 db_env=it_table.db_env,
                                 values=values)
            self.run_query(query, self.table)
            success = True
            msg = 'Inserted new record {db} {table} {db_env} into {it_table}'
            msg = msg.format(db=it_table.database, table=it_table.table_name,
                             it_table=self.table, db_env=it_table.db_env)
            self.logger.info(msg)
        else:
            msg = ('Table {db} {tbl} {db_env} NOT INSERTED into {it_table}.'
                   ' Already exists!')
            msg = msg.format(db=it_table.database, tbl=it_table.table_name,
                             it_table=self.table, db_env=it_table.db_env)
            self.logger.warning(msg)
        return success, msg

    def insert_placeholder(self, db_name, table_name):
        """Insert a placeholder row, containing source_database_name,
        source_table_name, and hold for a new table request into IT table.
        """
        tbl_dict = {
            'full_table_name': '', 'domain': '', 'target_dir': '',
            'split_by': '', 'mappers': 0, 'jdbcurl': '',
            'connection_factories': '', 'db_username': '', 'password_file': '',
            'load': '', 'fetch_size': 0, 'hold': 1, 'esp_appl_id': '',
            'views': '', 'esp_group': '', 'check_column': '',
            'source_schema_name': '', 'sql_query': '', 'actions': '',
            'source_database_name': db_name, 'source_table_name': table_name,
            'db_env': self.cfg_mgr.default_db_env.lower()}
        it_table_row = ItTable(tbl_dict, self.cfg_mgr)
        self.insert(it_table_row)

    def drop_partition(self, db_name, table_name):
        """Drops a partition from it table"""
        query = ("alter table {it_tbl} drop if exists partition "
                 "(source_database_name='{db_name}', "
                 "source_table_name='{tbl_name}')")
        query = query.format(it_tbl=self.table, db_name=db_name,
                             tbl_name=table_name)
        self.run_query(query, self.table)
        warn_msg = 'Table {db} {tbl} dropped from {it_table}'
        warn_msg = warn_msg.format(db=db_name, tbl=table_name,
                                   it_table=self.table)
        self.logger.warning(warn_msg)

    def update(self, it_table):
        """Updates/overwrites a record with a provided
        dictionary representation of a table
        Args:
            it_table: instance of ibis.model.table.ItTable. it_table row
        """
        updated = False
        where_condition = ("source_database_name='{0}' and "
                           "source_table_name='{1}' and "
                           "db_env='{2}'")
        where_condition = where_condition.format(
            it_table.database, it_table.table_name, it_table.db_env)

        if self.get_all(where_condition):
            values = self._build_values_clause(it_table)
            query = ("INSERT OVERWRITE TABLE {it_tbl} PARTITION "
                     "(source_database_name='{db}', source_table_name"
                     "='{tbl}' , db_env='{db_env}') VALUES ({values})")
            query = query.format(it_tbl=self.table, db=it_table.database,
                                 tbl=it_table.table_name,
                                 db_env=it_table.db_env,
                                 values=values)
            self.run_query(query, self.table)
            updated = True
            msg = 'Updated table {db} {table} {db_env} in {it_table}'
            msg = msg.format(db=it_table.database, table=it_table.table_name,
                             it_table=self.table, db_env=it_table.db_env)
            self.logger.info(msg)
        else:
            msg = ('Table {db} {tbl} {db_env} NOT UPDATED in {it_table}.'
                   ' It does not exist!')
            msg = msg.format(db=it_table.database, tbl=it_table.table_name,
                             it_table=self.table, db_env=it_table.db_env)
            self.logger.warning(msg)
        return updated, msg

    def parse_requests(self, requests):
        """Return a list of dictionaries of table representation.
        Args:
            requests: List of ibis.inventory.request_inventory.Request
        Returns:
            List[{ibis.model.table.ItTable}].
        """
        tables = []
        for request_table in requests:
            it_table = ItTable(request_table.get_meta_data(), self.cfg_mgr)
            tables.append(it_table)
        return tables

    def _build_row_dict(self, row):
        """Builds it_table row dict
        Args:
            row: list of row values from it_table
        """
        tbl_dict = {
            'full_table_name': row[0], 'domain': row[1], 'target_dir': row[2],
            'split_by': row[3], 'mappers': row[4], 'jdbcurl': row[5],
            'connection_factories': row[6], 'db_username': row[7],
            'password_file': row[8], 'load': row[9], 'fetch_size': row[10],
            'hold': row[11], 'esp_appl_id': row[12], 'views': row[13],
            'esp_group': row[14], 'check_column': row[15],
            'source_schema_name': row[16], 'sql_query': row[17],
            'actions': row[18],
            'source_database_name': row[-3], 'source_table_name': row[-2],
            'db_env': row[-1]}
        return tbl_dict

    def get_all_tables(self):
        """Return a list of all records from it_table."""
        tables = []
        query = 'SELECT * FROM {tbl}'.format(tbl=self.table)
        result = self.get_rows(query)
        if result:
            for table_row in result:
                tables.append(self._build_row_dict(table_row))
        return tables

    def save_all_tables(self, file_name, source_type):
        """Saves records in IT Table to a text file"""
        _path = self.cfg_mgr.files + '{name}.txt'.format(name=file_name)
        file_h = open(os.path.join(_path), "wb+")

        tables = self.get_all_tables()
        cnt = 0
        for tbl in tables:
            table = ItTable(tbl, self.cfg_mgr)
            if source_type:
                if source_type not in table.jdbcurl.lower():
                    continue
            cnt += 1
            file_h.write('\n[Request]\n')
            file_h.write('domain:{0}\n'.format(table.domain))
            file_h.write('jdbcurl:{0}\n'.format(table.jdbcurl))
            file_h.write('db_username:{0}\n'.format(table.username))
            file_h.write('password_file:{0}\n'.format(table.password_file))
            file_h.write('split_by:{0}\n'.format(table.split_by))
            file_h.write('mappers:{0}\n'.format(table.mappers))
            file_h.write('refresh_frequency:{0}\n'.format(
                table.frequency_readable))
            file_h.write('weight:{0}\n'.format(
                table.load_readable))
            file_h.write('fetch_size:{0}\n'.format(table.fetch_size))
            file_h.write('hold:{0}\n'.format(table.hold))
            file_h.write('esp_appl_id:{0}\n'.format(table.esp_appl_id))
            file_h.write('views:{0}\n'.format(table.views))
            file_h.write('esp_group:{0}\n'.format(table.esp_group))
            file_h.write('check_column:{0}\n'.format(table.check_column))
            file_h.write('source_schema_name:{0}\n'.format(
                table.schema))
            file_h.write('sql_query:{0}\n'.format(table.query))
            file_h.write('actions:{0}\n'.format(table.actions))
            file_h.write('source_database_name:{0}\n'.format(
                table.database))
            file_h.write('source_table_name:{0}\n'.format(
                table.table_name))
            file_h.write('db_env:{0}\n'.format(
                table.db_env))
        file_h.close()
        msg = 'Saved {count} tables in {it_table} to {path}{file_name}.txt'
        msg = msg.format(count=cnt, it_table=self.table,
                         path=self.cfg_mgr.files, file_name=file_name)
        self.logger.info(msg)
        return msg

    def get_all_views(self):
        """Fetch all views from it_table"""
        pass

    def get_domains(self):
        """Get all distinct domains in the IT table."""
        domains = []
        query = 'SELECT DISTINCT domain FROM {tbl}'.format(tbl=self.table)
        result = self.get_rows(query)
        if result:
            for dom in result:
                domain = dom[0]
                domains.append(domain)
        return domains

    def get_all_tables_for_esp(self, esp_id):
        """Returns a list[table] of all sql-tables that match with ESP id"""
        tables = []
        query = "SELECT * FROM {tbl} WHERE esp_appl_id='{id}'"
        query = query.format(tbl=self.table, id=esp_id)
        result = self.get_rows(query)
        if result:
            for table_row in result:
                tbl_dict = self._build_row_dict(table_row)
                tables.append(ItTable(tbl_dict, self.cfg_mgr))
        return tables

    def get_all_tables_for_domain(self, domain):
        """Return all the sql-tables for a domain as a List{Table}."""
        domain_tables = []
        query = "SELECT * FROM {tbl} WHERE domain='{domain}'"
        query = query.format(tbl=self.table, domain=domain)
        result = self.get_rows(query)
        if result:
            for table_row in result:
                tbl_dict = self._build_row_dict(table_row)
                domain_tables.append(tbl_dict)
        return domain_tables

    def get_all_tables_for_schedule(self, schedule):
        """Returns all the sql-tables with schedule"""
        schedule_tables = []
        allowed_frequencies = self.cfg_mgr.allowed_frequencies
        if schedule in allowed_frequencies.values():
            # Get key associated with frequency name
            freq_val = allowed_frequencies.keys()[
                allowed_frequencies.values().index(schedule)]
            tables = self.get_all_tables()
            for table in tables:
                freq = table['load'][0:3]
                if freq == freq_val:
                    schedule_tables.append(table)
                else:
                    err_msg = 'Table, {table}, has incomplete load value'
                    err_msg = err_msg.format(table=table['full_table_name'])
                    self.logger.error(err_msg)
        else:
            err_msg = 'Unsupported schedule please choose from {0}'
            err_msg = err_msg.format(self.cfg_mgr.allowed_frequencies.values)
            self.logger.error(err_msg)
        return schedule_tables
