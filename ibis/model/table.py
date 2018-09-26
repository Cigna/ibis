"""ORM for it_table"""
import uuid
import copy
import re
from datetime import datetime
from ibis.custom_logging import get_logger
from ibis.utilities.utilities import Utilities
from ibis.utilities.sqoop_helper import DRIVERS, \
    ORACLE, DB2, TERADATA, SQLSERVER, MYSQL, POSTGRESQL, VALID_SOURCES


class ItTable(object):
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
        self._props = ['domain', 'split_by', 'mappers', 'jdbcurl',
                       'target_dir', 'schema', 'query', 'username',
                       'password_file', 'load', 'frequency', 'fetch_size',
                       'hold', 'esp_appl_id', 'views', 'esp_group',
                       'check_column', 'table_name', 'database', 'db_env']

    def null_helper(self, val):
        """Convert None to empty string"""
        if val is None:
            val = ''
        return val

    @property
    def domain(self):
        """Returns the domain"""
        val = self.get_meta_dict().get('domain', '')
        return self.null_helper(val).lower()

    @domain.setter
    def domain(self, val):
        """setter for domain url"""
        self.meta_dict['domain'] = val

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
    def split_by(self):
        """Returns the split by"""
        val = self.get_meta_dict().get('split_by', '')
        return self.null_helper(val)

    @split_by.setter
    def split_by(self, val):
        """setter for split_by"""
        self.meta_dict['split_by'] = val

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
        """setter for jdbc url"""
        self.meta_dict['jdbcurl'] = val

    @property
    def target_dir(self):
        """Returns the target directory"""
        _val = '{root_saves}/{domain}/{database}/{table}'
        _val = _val.format(
            root_saves=self.cfg_mgr.root_hdfs_saves, domain=self.domain,
            database=Utilities.replace_special_chars(self.database),
            table=Utilities.replace_special_chars(self.table_name))
        return _val

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
        windows_auth = 'domain'
        if ORACLE in self.jdbcurl:
            _driver = DRIVERS[ORACLE]
        elif DB2 in self.jdbcurl:
            _driver = DRIVERS[DB2]
        elif TERADATA in self.jdbcurl:
            _driver = DRIVERS[TERADATA]
        elif SQLSERVER in self.jdbcurl and windows_auth in self.jdbcurl:
            _driver = DRIVERS[SQLSERVER][1]
        elif SQLSERVER in self.jdbcurl:
            _driver = DRIVERS[SQLSERVER][0]
        elif POSTGRESQL in self.jdbcurl:
            _driver = DRIVERS[POSTGRESQL]
        elif MYSQL in self.jdbcurl:
            _driver = DRIVERS[MYSQL]
        return _driver

    @property
    def is_oracle(self):
        """Returns the source of the table"""
        valid_source = [True for source in VALID_SOURCES
                        if source in self.jdbcurl]

        if valid_source or self.jdbcurl in ['', 'null']:
            return bool(ORACLE in self.jdbcurl)
        else:
            raise ValueError("Unrecognized source found "
                             "in: '{0}'".format(self.jdbcurl))

    @property
    def is_teradata(self):
        """Returns the source of the table"""
        valid_source = [True for source in VALID_SOURCES
                        if source in self.jdbcurl]

        if valid_source or self.jdbcurl in ['', 'null']:
            return bool(TERADATA in self.jdbcurl)
        else:
            raise ValueError("Unrecognized source found "
                             "in: '{0}'".format(self.jdbcurl))

    @property
    def is_sqlserver(self):
        """Returns the source of the table"""
        valid_source = [True for source in VALID_SOURCES
                        if source in self.jdbcurl]

        if valid_source or self.jdbcurl in ['', 'null']:
            return bool(SQLSERVER in self.jdbcurl)
        else:
            raise ValueError("Unrecognized source found "
                             "in: '{0}'".format(self.jdbcurl))

    @property
    def is_postgresql(self):
        """Returns the source of the table"""
        valid_source = [True for source in VALID_SOURCES
                        if source in self.jdbcurl]

        if valid_source or self.jdbcurl in ['', 'null']:
            return bool(POSTGRESQL in self.jdbcurl)
        else:
            raise ValueError("Unrecognized source found "
                             "in: '{0}'".format(self.jdbcurl))

    @property
    def is_mysql(self):
        """Returns the source of the table"""
        valid_source = [True for source in VALID_SOURCES
                        if source in self.jdbcurl]

        if valid_source or self.jdbcurl in ['', 'null']:
            return bool(MYSQL in self.jdbcurl)
        else:
            raise ValueError("Unrecognized source found"
                             "in: '{0}'".format(self.jdbcurl))

    @property
    def is_db2(self):
        """Returns the source of the table"""
        valid_source = [True for source in VALID_SOURCES
                        if source in self.jdbcurl]
        if valid_source or self.jdbcurl in ['', 'null']:
            return bool(DB2 in self.jdbcurl)
        else:
            raise ValueError("Unrecognized source found "
                             "in: '{0}'".format(self.jdbcurl))

    @connection_factories.setter
    def connection_factories(self, val):
        """setter"""
        self.meta_dict['connection_factories'] = val

    @property
    def frequency_load(self):
        """Return frequency and load Ex:100100"""
        default_val = '000001'
        val = self.meta_dict.get('load', default_val)
        if val == '':
            val = default_val
        return val

    @property
    def load(self):
        """Returns the table load value"""
        _load = '100'
        try:
            _load = self.meta_dict.get('load', '000001')
            if len(_load) == 6:
                _load = _load[3:6]
            else:
                raise IndexError('corrupted load value')
        except IndexError:
            err_msg = "{table}: Index error in retrieving " \
                      "load value from value '{load}'"
            err_msg = err_msg.format(table=self.table_name,
                                     load=_load)
            self.logger.error(err_msg)
        return _load

    @load.setter
    def load(self, val):
        """setter for load"""
        # test if it is binary
        _ = int(val, base=2)
        self.meta_dict['load'] = self.frequency + val

    @property
    def load_readable(self):
        """Return readable frequency value"""
        _load = self.load
        value = ItTable.load_readable_helper(_load)
        return value

    @load_readable.setter
    def load_readable(self, value):
        """setter for load_readable"""
        _load = ItTable.load_binary_helper(value)
        self.load = _load

    @property
    def is_heavy(self):
        """is it a heavy load table"""
        return bool(self.load_readable == 'heavy')

    @property
    def is_medium(self):
        """is it a medium load table"""
        return bool(self.load_readable == 'medium')

    @property
    def is_small(self):
        """is it a small load table"""
        return bool(self.load_readable == 'small')

    @classmethod
    def load_readable_helper(cls, _load):
        """
        Args:
            _load: binary value
        Return: readable value
        """
        value = 'none'
        if _load == '100':
            value = 'small'
        elif _load == '010':
            value = 'medium'
        elif _load == '001':
            value = 'heavy'
        return value

    @classmethod
    def load_binary_helper(cls, value):
        """
        Args:
            value: readable value
        Return: binary value
        """
        if value == '':
            _load = '001'
        elif value == 'small':
            _load = '100'
        elif value == 'medium':
            _load = '010'
        elif value == 'heavy':
            _load = '001'
        else:
            raise ValueError("Unrecognized load: '{0}'".format(value))
        return _load

    @property
    def frequency(self):
        """Returns the frequency value for job updates"""
        _load = self.meta_dict.get('load', '000001')
        freq = '000'
        try:
            if len(_load) == 6:
                freq = _load[0:3]
            else:
                raise IndexError('corrupted load value')
        except IndexError:
            err_msg = '{table}: Index error in retrieving ' \
                      'frequency from value {load}'
            err_msg = err_msg.format(table=self.table_name,
                                     load=_load)
            self.logger.error(err_msg)
        return freq

    @frequency.setter
    def frequency(self, val):
        """setter for frequency"""
        # test if it is binary
        _ = int(val, base=2)
        self.meta_dict['load'] = val + self.load

    @property
    def frequency_readable(self):
        """Return readable frequency value"""
        _freq = self.frequency
        value = ItTable.frequency_readable_helper(_freq)
        return value

    @frequency_readable.setter
    def frequency_readable(self, value):
        """setter for frequency_readable"""
        self.frequency = ItTable.frequency_binary_helper(value)

    @classmethod
    def frequency_readable_helper(cls, _freq):
        """
        Args:
            _freq: binary value
        Return: readable value
        """
        value = 'none'
        if _freq == '000':
            value = 'none'
        elif _freq == '101':
            value = 'daily'
        elif _freq == '100':
            value = 'weekly'
        elif _freq == '011':
            value = 'biweekly'
        elif _freq == '110':
            value = 'fortnightly'
        elif _freq == '010':
            value = 'monthly'
        elif _freq == '001':
            value = 'quarterly'
        elif _freq == '111':
            value = 'yearly'
        return value

    @classmethod
    def frequency_binary_helper(cls, value):
        """
        Args:
            value: readable value
        Return: binary value
        """
        if value == '':
            _freq = '000'
        elif value == 'none':
            _freq = '000'
        elif value == 'daily':
            _freq = '101'
        elif value == 'weekly':
            _freq = '100'
        elif value == 'biweekly':
            _freq = '011'
        elif value == 'fortnightly':
            _freq = '110'
        elif value == 'monthly':
            _freq = '010'
        elif value == 'quarterly':
            _freq = '001'
        elif value == 'yearly':
            _freq = '111'
        else:
            raise ValueError("Unrecognized frequency: '{0}'".format(value))
        return _freq

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
    def hold(self):
        """Returns the hold value."""
        _default = 0
        val = self.get_meta_dict().get('hold',
                                       _default)
        val = self.null_helper(val)
        if val:
            val = int(val)
        else:
            val = _default
        return val

    @hold.setter
    def hold(self, val):
        """Hold setter 0-not hold, 1-hold"""
        if val == '':
            val = 0
        self.meta_dict['hold'] = int(val)

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
    def table_name(self):
        """Returns the table name"""
        val = self.meta_dict.get('source_table_name', '')
        if not (self.is_sqlserver or self.is_mysql):
            val = val.lower()
        return val

    @property
    def database(self):
        """Returns the database name"""
        val = self.meta_dict.get('source_database_name', '')
        if not (self.is_sqlserver or self.is_mysql):
            val = val.lower()
        return val

    # @property
    # def sqlserver_database(self):
    #    """Extract database from jdbcurl"""
    #    database = self.database
    #    if self.is_sqlserver:
    #        db_name = self.jdbcurl.split('database=')[1]
    #       database = db_name.split(';encrypt')[0]
    #    return database.lower()

    @property
    def db_table_name(self):
        """Returns the db and table name"""
        return self.database + '_' + self.table_name

    @property
    def full_sql_name(self):
        """Returns db.table name"""
        val = 'unknown'
        if self.is_sqlserver:
            val = self.database + '.' + self.schema + '.' + self.table_name
        elif self.is_mysql:
            val = self.database + '.' + self.table_name
        elif self.is_postgresql:
            val = self.database + '.' + self.schema + '.' + self.table_name
        else:
            val = self.database.upper() + '.' + self.table_name.upper()
        return val

    @property
    def full_name(self):
        """Returns the the full table names"""
        _val = self.domain + '.' + self.database + '_' + self.table_name
        return _val

    @property
    def table_id(self):
        """getter for unique id for the source table"""
        return self.db_env + ': ' + self.full_sql_name

    def views_helper(self, _views):
        """helper"""
        if isinstance(_views, list):
            pass
        elif isinstance(_views, str) and _views:
            reg_lst = r'[^A-Za-z0-9|_]'
            strsrch = re.search(reg_lst, _views)

            if strsrch is None:
                _views = _views.split('|')
            else:
                msg = ("characters other than Alphabets and Pipe and "
                       "underscore is not allowed in view "
                       "value: '{0}'").format(_views)
                raise ValueError(Utilities.print_box_msg(msg, border_char='x'))

        elif not _views:
            _views = []
        else:
            raise ValueError("Views is a pipe separated "
                             "value: '{0}'".format(_views))
        return _views

    @property
    def views(self):
        """| seperated views getter"""
        return '|'.join(self.views_list)

    @views.setter
    def views(self, val):
        """setter for views"""
        _domains = ['domain1', 'domain2', 'domain3']
        _views = self.views_list
        new_views = self.views_helper(val)

        if len(new_views) == 0:
            _views = new_views
        else:
            _views = _views + new_views

        # deduplicate
        _views = sorted(set(_views), key=_views.index)
        common_domains = set(_views).intersection(_domains)

        if len(common_domains) == 0 and val != '' and False:
            err_msg = ("\nMust include a data domain in the views "
                       "field from following: {0}\nYou provided: {1}")
            err_msg = err_msg.format(_domains, _views)
            raise ValueError(err_msg)

        self.meta_dict['views'] = '|'.join(_views)

    @property
    def views_list(self):
        """list of views"""
        _views = self.get_meta_dict().get('views', '')
        return self.views_helper(_views)

    @property
    def view_tables(self):
        """List of hive tables for views"""
        hive_tables = []
        views = self.views_list
        if views:
            for view in views:
                _table = view + '.' + self.db_table_name
                hive_tables.append(_table)
        return hive_tables

    @property
    def esp_group(self):
        """esp_group getter"""
        val = self.get_meta_dict().get('esp_group', '')
        val = self.null_helper(val)
        return val

    @esp_group.setter
    def esp_group(self, val):
        """setter for esp_group"""
        self.meta_dict['esp_group'] = val

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

    @property
    def query(self):
        """getter for custom ingest query"""
        val = self.get_meta_dict().get('sql_query', '')
        val = self.null_helper(val)
        return val

    @query.setter
    def query(self, val):
        """custom ingest query setter"""
        self.meta_dict['sql_query'] = val

    @property
    def check_column(self):
        """check_column getter"""
        val = self.get_meta_dict().get('check_column', '')
        val = self.null_helper(val)
        return val

    @check_column.setter
    def check_column(self, val):
        """setter for check_column"""
        self.meta_dict['check_column'] = val

    @property
    def actions(self):
        """getter for DSL actions file path in ibis/request-files git repo"""
        val = self.get_meta_dict().get('actions', '')
        val = self.null_helper(val)
        return val

    @actions.setter
    def actions(self, val):
        """setter for actions"""
        self.meta_dict['actions'] = val

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
        """equality"""
        status = True
        if not isinstance(other, self.__class__):
            return False
        for prp in self._props:
            prop1 = getattr(self, prp)
            prop2 = getattr(other, prp)
            if prop1 != prop2:
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
        text = '\nIt table'
        for attr in dir(self):
            if isinstance(getattr(type(self), attr, None),
                          property):
                msg = "\n\t{property_name}: '{value}'"
                text += \
                    msg.format(property_name=attr, value=getattr(self, attr))
        text += '\n====================='
        return text
