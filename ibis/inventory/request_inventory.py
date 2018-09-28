"""Handles request file."""
import copy
from ibis.custom_logging import get_logger
from ibis.inventory.it_inventory import ITInventory
from ibis.inventory.export_it_inventory import ExportITInventory
from ibis.model.table import ItTable
from ibis.utilities.file_parser import parse_file_by_sections
from ibis.model.exporttable import ItTableExport


# List of keys required in a [Request] section
REQUIRED_FIELDS = ['source_database_name', 'source_table_name']
OPTIONAL_FIELDS = ['jdbcurl', 'db_username', 'password_file',
                   'mappers', 'refresh_frequency', 'weight', 'views',
                   'check_column', 'esp_group', 'split_by', 'fetch_size',
                   'hold', 'esp_appl_id', 'source_schema_name', 'sql_query',
                   'actions', 'db_env']

REQUIRED_FIELDS_EXPORT = ['source_database_name', 'source_table_name',
                          'jdbcurl', 'target_schema',
                          'target_table', 'db_username', 'password_file']
OPTIONAL_FIELDS_EXPORT = ['update_key', 'fetch_size',
                          'source_dir', 'refresh_frequency', 'mappers',
                          'esp_appl_id', 'weight', 'db_env',
                          'staging_database']


class Request(object):
    """Represents fields in request file"""

    def __init__(self, meta_dict, cfg_mgr):
        """Init"""
        self.cfg_mgr = cfg_mgr
        self.meta_dict = copy.deepcopy(meta_dict)
        # check for allowed frequencies
        # self.frequency_readable = self.frequency_readable
        self.logger = get_logger(self.cfg_mgr)

    @property
    def username(self):
        """getter"""
        return self.get_meta_data().get('db_username', False)

    @property
    def db_env(self):
        """getter"""
        val = self.get_meta_data().get('db_env')
        if val:
            val = val.lower()
        else:
            val = False
        return val

    @property
    def target_schema(self):
        """getter"""
        return self.get_meta_data().get('target_schema', False)

    @property
    def update_key(self):
        """getter"""
        return self.get_meta_data().get('update_key', False)

    @property
    def target_table(self):
        """getter"""
        return self.get_meta_data().get('target_table', False)

    @property
    def source_dir(self):
        """getter"""
        return self.get_meta_data().get('source_dir', False)

    @property
    def password_file(self):
        """getter"""
        return self.get_meta_data().get('password_file', False)

    @property
    def jdbcurl(self):
        """getter"""
        return self.get_meta_data().get('jdbcurl', False)

    @property
    def staging_database(self):
        """getter"""
        return self.get_meta_data().get('staging_database', False)

    @property
    def database(self):
        """getter"""
        val = self.get_meta_data().get('source_database_name')
        if self.jdbcurl:
            if not ('mysql' in self.jdbcurl or 'sqlserver' in self.jdbcurl or
                    'postgresql' in self.jdbcurl):
                val = val.lower()
        return val

    @property
    def table_name(self):
        """getter"""
        val = self.get_meta_data().get('source_table_name')
        if self.jdbcurl:
            if not('mysql' in self.jdbcurl or 'sqlserver' in self.jdbcurl or
                   'postgresql' in self.jdbcurl):
                val = val.lower()
        return val

    @property
    def db_table_name(self):
        """Returns the db and table name"""
        return self.database + '_' + self.table_name

    @property
    def frequency_readable(self):
        """getter
        Returns:
            False: when refresh_frequency is missing in request file
        """
        _val = self.get_meta_data().get('refresh_frequency', False)
        if isinstance(_val, str):
            _val = _val.lower()
        return _val

    # @frequency_readable.setter
    # def frequency_readable(self, val):
    #    """setter"""
    #    allowed_freqs = self.cfg_mgr.allowed_frequencies.values() + \
    #        ['null', False]
    #    if val not in allowed_freqs:
    #        msg = "Requested frequency: '{freq}' is not"
    #              " allowed for {db} {tbl}"
    #        msg += "\nAllowed: {allowed}"
    #        msg = msg.format(freq=val, db=self.database, tbl=self.table_name,
    #                         allowed=allowed_freqs)
    #        raise ValueError(msg)
    #    self.meta_dict['refresh_frequency'] = val

    @property
    def load_readable(self):
        """getter for load/weight
        Returns:
            False: when load is missing in request file
        """
        _val = self.get_meta_data().get('weight', False)
        if isinstance(_val, str):
            _val = _val.lower()
        return _val

    @property
    def views(self):
        """getter
        Returns:
            False: when views is missing in request file
        """
        _val = self.get_meta_data().get('views', False)
        if isinstance(_val, str):
            _val = _val.lower()
        return _val

    @property
    def split_by(self):
        """getter
        Returns:
            False: when split_by is missing in request file
        """
        return self.get_meta_data().get('split_by', False)

    @property
    def fetch_size(self):
        """getter
        Returns:
            False: when fetch_size is missing in request file
        """
        return self.cast_int(self.get_meta_data().get('fetch_size', False))

    @property
    def hold(self):
        """getter
        Returns:
            False: when hold is missing in request file
        """
        return self.cast_int(self.get_meta_data().get('hold', False))

    @property
    def check_column(self):
        """getter
        Returns:
            False: when check_column is missing in request file
        """
        return self.get_meta_data().get('check_column', False)

    @property
    def esp_group(self):
        """getter
        Returns:
            False: when esp_group is missing in request file
        """
        return self.get_meta_data().get('esp_group', False)

    @property
    def esp_appl_id(self):
        """getter
        Returns:
            False: when esp_appl_id is missing in request file
        """
        return self.get_meta_data().get('esp_appl_id', False)

    @property
    def mappers(self):
        """getter
        Returns:
            False: when mappers is missing in request file
        """
        return self.cast_int(self.get_meta_data().get('mappers', False))

    @property
    def schema(self):
        """getter
        Returns:
            False: when schema is missing in request file
        """
        return self.get_meta_data().get('source_schema_name', False)

    @property
    def query(self):
        """getter
        Returns:
            False: when query is missing in request file
        """
        return self.get_meta_data().get('sql_query', False)

    @property
    def actions(self):
        """getter
        Returns:
            False: when actions is missing in request file
        """
        return self.get_meta_data().get('actions', False)

    def cast_int(self, val):
        """return int of val"""
        if val not in [False, 'null']:
            val = int(val)
        return val

    def get_meta_data(self):
        """get meta data"""
        # caution: dict is mutable type
        return copy.deepcopy(self.meta_dict)

    def fieldExists(self, field_name):
        """Checks if user request has this field set"""
        if getattr(self, field_name) is False:
            return False
        else:
            return True

    def __repr__(self):
        """Obj repr. Prints only python properties"""
        text = '\n'
        for attr in dir(self):
            if isinstance(getattr(type(self), attr, None), property):
                msg = "{property_name}: {value}\n"
                text += msg.format(property_name=attr,
                                   value=getattr(self, attr))
        return text


class RequestInventory(object):
    """Handles request file"""

    def __init__(self, cfg_mgr):
        """Init."""
        self.cfg_mgr = cfg_mgr
        self.it_inventory = ITInventory(self.cfg_mgr)
        self.export_it_inventory = ExportITInventory(self.cfg_mgr)
        self.logger = get_logger(self.cfg_mgr)
        self.log_msg = ''

    def parse_file(self, request_file):
        """
        Args:
            request_file: instance of open()
        Returns:
            list(ibis.inventory.request_inventory.Request)
        """
        requests, msg, log_msg = parse_file_by_sections(
            request_file, '[Request]', REQUIRED_FIELDS, OPTIONAL_FIELDS)
        self.logger.warning(log_msg)
        self.log_msg = log_msg
        temp_reqs = []
        if not msg:
            temp_reqs = [Request(req, self.cfg_mgr) for req in requests]
        return temp_reqs, msg

    def parse_file_export(self, request_file):
        """
        Args:
            request_file: instance of open()
        Returns:
            list(ibis.inventory.request_inventory.Request)
        """
        requests, msg, log_msg = parse_file_by_sections(
            request_file, '[Request]',
            REQUIRED_FIELDS_EXPORT, OPTIONAL_FIELDS_EXPORT)
        self.logger.warning(log_msg)
        self.log_msg = log_msg
        temp_reqs = []
        if not msg:
            temp_reqs = [Request(req, self.cfg_mgr) for req in requests]
        return temp_reqs, msg

    def get_available_requests(self, requests):
        """Given a list[{Request}] return a List[{ItTable}]
        of tables available, not on hold in the it table, and a
        List[{ItTable}] of tables on hold, due to a hold value
        of 1 and List[{Request}]
        Args:
            requests: list(ibis.inventory.request_inventory.Request) objects
        Returns:
            list(ibis.model.table.ItTable),
            list(ibis.model.table.ItTable),
            list(ibis.inventory.request_inventory.Request)
        """
        available_tables = []
        hold_tables = []
        unavailable_requests = []
        for req_obj in requests:
            src_db = req_obj.database
            src_table = req_obj.table_name
            if req_obj.db_env:
                db_env = req_obj.db_env
            else:
                db_env = self.cfg_mgr.default_db_env.lower()
            table = self.it_inventory.get_table_mapping(src_db, src_table,
                                                        db_env)
            if table:
                # A record of the table exists in the it table
                if table['hold'] == 0:
                    available_tables.append(ItTable(table, self.cfg_mgr))
                else:
                    # hold = 1 means it is disabled temporarily
                    hold_tables.append(ItTable(table, self.cfg_mgr))
            else:
                # Record doesn't exist
                unavailable_requests.append(req_obj)
        if hold_tables:
            hold_tables_names = [table.table_name for table in hold_tables]
            msg = 'Tables: {0} are on hold'
            msg = msg.format(', '.join(hold_tables_names))
            self.logger.warning(msg)
        if unavailable_requests:
            unavail_names = [req.table_name for req in unavailable_requests]
            msg = 'Tables: {0} are missing in it_table'
            msg = msg.format(', '.join(unavail_names))
            self.logger.warning(msg)
        return available_tables, hold_tables, unavailable_requests

    def get_available_requests_export(self, requests):
        """Given a list[{Request}] return a List[{ItTableExport}]
        of tables available
        Args:
            requests: list(ibis.inventory.request_inventory.Request) objects
        Returns:
            list(ibis.model.table.ItTableExport),
            list(ibis.model.table.ItTableExport),
            list(ibis.inventory.request_inventory.Request)
        """
        tables = []
        unavailable_requests = []
        for req_obj in requests:
            src_db = req_obj.database
            src_table = req_obj.table_name
            if req_obj.db_env:
                db_env = req_obj.db_env
            else:
                db_env = self.cfg_mgr.default_db_env.lower()
            table = self.export_it_inventory.get_table_mapping(src_db,
                                                               src_table,
                                                               db_env)
            if table:
                # A record of the table exists in the it table
                tables.append(ItTableExport(table, self.cfg_mgr))
            else:
                # Record doesn't exist
                unavailable_requests.append(req_obj)
        if unavailable_requests:
            unavail_names = [req.table_name for req in unavailable_requests]
            msg = 'Tables: {0} are missing in it table'
            msg = msg.format(', '.join(unavail_names))
            self.logger.warning(msg)
        return tables, unavailable_requests
