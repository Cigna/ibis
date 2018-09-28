"""ORM for it_table"""
import copy
from ibis.custom_logging import get_logger
from ibis.utilities.sqoop_helper import DRIVERS, \
    ORACLE, DB2, TERADATA, SQLSERVER, MYSQL, VALID_SOURCES


class ItTableExport(object):
    """Table representation of columns of it_table"""

    def __init__(self, meta_dict, cfg_mgr):
        """Init
        Args:
            meta_dict: A hive db record {column_name: value}
            cfg_mgr: instance of ibis.utilities.config_manager.ConfigManager
        """
        self.cfg_mgr = cfg_mgr
        self.logger = get_logger(self.cfg_mgr)
        self.meta_dict = copy.deepcopy(meta_dict)
        self._props = ['mappers', 'jdbcurl',
                       'source_dir', 'schema', 'username',
                       'password_file', 'load', 'frequency', 'fetch_size',
                       'esp_appl_id',
                       'table_name', 'database', 'target_schema',
                       'target_table', 'weight', 'db_env', 'staging_database']

    def null_helper(self, val):
        """Convert None to empty string"""
        if val is None:
            val = ''
        return val

    @property
    def mappers(self):
        """Returns the number of mappers"""
        _default = 10
        val = self.get_meta_dict().get('mappers', _default)
        val = self.null_helper(val)
        if val:
            val = int(val)
        else:
            val = _default
        return val

    @mappers.setter
    def mappers(self, val):
        """setter for mappers"""
        self.meta_dict['mappers'] = val

    @property
    def jdbcurl(self):
        """Returns the jdbcurl"""
        val = self.get_meta_dict().get('jdbcurl', '')
        return self.null_helper(val)

    @jdbcurl.setter
    def jdbcurl(self, val):
        """setter for jdbcurl"""
        self.meta_dict['jdbcurl'] = val

    @property
    def staging_database(self):
        """Returns the staging_database"""
        val = self.get_meta_dict().get('staging_database', '')
        return self.null_helper(val)

    @staging_database.setter
    def staging_database(self, val):
        """setter for staging_database"""
        self.meta_dict['staging_database'] = val

    @property
    def db_env(self):
        """Returns the db_env"""
        default_val = self.cfg_mgr.default_db_env.lower()
        val = self.get_meta_dict().get('db_env', '')
        if val == '':
            val = default_val
        return self.null_helper(val).lower()

    @db_env.setter
    def db_env(self, val):
        """setter for db_env"""
        self.meta_dict['db_env'] = val

    @property
    def esp_appl_id(self):
        """Return esp appl id"""
        val = self.get_meta_dict().get('esp_appl_id',
                                       '')
        val = self.null_helper(val)
        return val

    @esp_appl_id.setter
    def esp_appl_id(self, val):
        """setter for esp_appl_id"""
        self.meta_dict['esp_appl_id'] = val

    @property
    def target_schema(self):
        """Returns the target_schema"""
        val = self.get_meta_dict().get('target_schema', '')
        return self.null_helper(val)

    @target_schema.setter
    def targetschema(self, val):
        """setter for target_schema"""
        self.meta_dict['target_schema'] = val

    @property
    def target_table(self):
        """Returns the target_table"""
        val = self.get_meta_dict().get('target_table', '')
        return self.null_helper(val)

    @target_table.setter
    def target_table(self, val):
        """setter for target_table"""
        self.meta_dict['target_table'] = val

    @property
    def weight(self):
        """Returns the weight"""
        val = self.get_meta_dict().get('weight', '')
        return self.null_helper(val)

    @weight.setter
    def weight(self, val):
        """setter for weight"""
        self.meta_dict['weight'] = val

    @property
    def update_key(self):
        """Returns the update key"""
        val = self.get_meta_dict().get('update_key', '')
        return self.null_helper(val)

    @update_key.setter
    def update_key(self, val):
        """setter for update_key"""
        self.meta_dict['update_key'] = val

    @property
    def source_dir(self):
        """Returns the target directory"""
        _val = '{root_saves}/{database}/{table}'
        _val = _val.format(
            root_saves=self.cfg_mgr.root_hdfs_saves,
            database=self.database, table=self.table_name)
        return _val

    @property
    def dir(self):
        """Returns the source_dir"""
        val = self.get_meta_dict().get('source_dir', '')
        return self.null_helper(val)

    @dir.setter
    def dir(self, val):
        """setter for source_dir"""
        self.meta_dict['source_dir'] = val

    @property
    def username(self):
        """Returns the db username"""
        val = self.get_meta_dict().get('db_username', '')
        return self.null_helper(val)

    @username.setter
    def username(self, val):
        """setter for db user name"""
        self.meta_dict['db_username'] = val

    @property
    def password_file(self):
        """Returns password file location"""
        val = self.get_meta_dict().get('password_file', '')
        return self.null_helper(val)

    @password_file.setter
    def password_file(self, val):
        """setter for password_file"""
        self.meta_dict['password_file'] = val

    @property
    def split_password_file(self):
        """Return jceks and password alias if exists"""
        if 'jceks' in self.password_file:
            jceks, password = self.password_file.split('#')
        else:
            jceks = None
            password = self.password_file
        return jceks, password

    @property
    def connection_factories(self):
        """Returns the connection factories"""
        _driver = ''
        if ORACLE in self.jdbcurl:
            _driver = DRIVERS[ORACLE]
        elif DB2 in self.jdbcurl:
            _driver = DRIVERS[DB2]
        elif TERADATA in self.jdbcurl:
            _driver = DRIVERS[TERADATA]
        elif SQLSERVER in self.jdbcurl:
            _driver = DRIVERS[SQLSERVER]
        elif MYSQL in self.jdbcurl:
            _driver = DRIVERS[MYSQL]
        return _driver

    @property
    def is_oracle(self):
        """Returns the source of the table"""
        valid_source = [True for source in VALID_SOURCES
                        if source in self.jdbcurl]

        if valid_source:
            return bool(ORACLE in self.jdbcurl)
        else:
            raise ValueError("Unrecognized source found "
                             "in: '{0}'".format(self.jdbcurl))

    @property
    def is_teradata(self):
        """Returns the source of the table"""
        valid_source = [True for source in VALID_SOURCES
                        if source in self.jdbcurl]

        if valid_source:
            return bool(TERADATA in self.jdbcurl)
        else:
            raise ValueError("Unrecognized source found "
                             "in: '{0}'".format(self.jdbcurl))

    @property
    def is_sqlserver(self):
        """Returns the source of the table"""
        valid_source = [True for source in VALID_SOURCES
                        if source in self.jdbcurl]

        if valid_source:
            return bool(SQLSERVER in self.jdbcurl)
        else:
            raise ValueError("Unrecognized source found "
                             "in: '{0}'".format(self.jdbcurl))

    @property
    def is_mysql(self):
        """Returns the source of the table"""
        valid_source = [True for source in VALID_SOURCES
                        if source in self.jdbcurl]
        if valid_source:
            if MYSQL in self.jdbcurl:
                return True
            else:
                return False
        else:
            raise ValueError("Unrecognized source found"
                             "in: '{0}'".format(self.jdbcurl))

    @property
    def is_db2(self):
        """Returns the source of the table"""
        valid_source = [True for source in VALID_SOURCES
                        if source in self.jdbcurl]
        if valid_source:
            return bool(DB2 in self.jdbcurl)
        else:
            raise ValueError("Unrecognized source found "
                             "in: '{0}'".format(self.jdbcurl))

    @connection_factories.setter
    def connection_factories(self, val):
        """setter"""
        self.meta_dict['connection_factories'] = val

    @property
    def load(self):
        """Returns the load"""
        val = self.get_meta_dict().get('load', '')
        return self.null_helper(val)

    @load.setter
    def load(self, val):
        """setter for load"""
        self.meta_dict['load'] = val

    @property
    def fetch_size(self):
        """Returns the fetch size"""
        _default = 50000
        val = self.get_meta_dict().get('fetch_size',
                                       _default)
        val = self.null_helper(val)
        if val:
            val = int(val)
        else:
            val = _default
        return val

    @fetch_size.setter
    def fetch_size(self, val):
        """setter for fetch size"""
        self.meta_dict['fetch_size'] = int(val) if val != '' else ''

    @property
    def table_name(self):
        """Returns the table name"""
        return self.meta_dict.get('source_table_name',
                                  '').lower()

    @property
    def table(self):
        """Returns the target table name"""
        return self.meta_dict.get('target_table_name',
                                  '').lower()

    @property
    def database(self):
        """Returns the database name"""
        return self.meta_dict.get('source_database_name',
                                  '').lower()

    @property
    def sqlserver_database(self):
        """Extract database from jdbcurl"""
        database = self.database
        if self.is_sqlserver:
            db_name = self.jdbcurl.split('database=')[1]
            database = db_name.split(';encrypt')[0]
        elif self.is_teradata:
            db_name = self.jdbcurl.split('database=')[1]
            database = db_name.split(';encrypt')[0]
        return database

    @property
    def db_table_name(self):
        """Returns the db and table name"""
        return self.database + '_' + self.table_name

    @property
    def full_sql_name(self):
        """Returns db.table name"""
        return self.database.upper() + '.' + self.table_name.upper()

    @property
    def full_name(self):
        """Returns the the full table names"""
        _val = self.database + '.' + self.table_name
        return _val

    @property
    def schema(self):
        """sql server schema getter"""
        val = self.get_meta_dict().get('source_schema_name', '')
        val = self.null_helper(val)
        return val

    @schema.setter
    def schema(self, val):
        """sqlserver schema setter"""
        self.meta_dict['source_schema_name'] = val

    def get_meta_dict(self):
        """Returns the value associated with the metadata table"""
        # caution: dict is mutable type
        return copy.deepcopy(self.meta_dict)

    def has_auth_info(self):
        """checks if the object has required info for sqoop auth"""
        if self.username and self.password_file and self.jdbcurl:
            return True
        return False

    def __eq__(self, other):
        status = True
        for prp in self._props:
            if getattr(self, prp) != getattr(other, prp):
                status = False
                msg = '{0} did not match. {1} != {2}'
                msg = msg.format(prp, getattr(self, prp),
                                 getattr(other, prp))
                print msg
                break
        return isinstance(other, self.__class__) and status

    def __ne__(self, obj):
        return not self == obj

    def __repr__(self):
        """Obj repr. Prints only python properties"""
        text = '\nIt table Export'
        for attr in dir(self):
            if isinstance(getattr(type(self), attr, None),
                          property):
                msg = "\n{property_name}: {value}"
                text += msg.format(property_name=attr,
                                   value=getattr(self, attr))
        text += '\n====================='
        return text
