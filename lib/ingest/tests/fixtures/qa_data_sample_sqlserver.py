import datetime

DATA_SAMPLE_HIVE_DDL = [
    ('login_id', 'varchar(32)', ''), ('sub_ssn', 'varchar(10)', ''),
    ('intrnl_user_id', 'int', ''), ('birth_dt', 'timestamp', ''),
    ('emp_id', 'varchar(10)', ''), ('rel_cd', 'char(2)', ''),
    ('ldap_uid', 'varchar(32)', ''), ('lan_id', 'varchar(8)', ''),
    ('last_updt_ts', 'timestamp', ''), ('last_updt_oper_id', 'varchar(8)', ''),
    ('email_addr', 'varchar(100)', ''), ('email_opt_cd', 'smallint', ''),
    ('ldap_refresh_ind', 'char(1)', ''), ('user_access_cd', 'char(1)', ''),
    ('ingest_timestamp', 'string', ''),
    ('incr_ingest_timestamp', 'string', '')]

DATA_SAMPLE_HIVE_DDL_EXPORT = [
    ('login_id', 'string(32)', '')]

DATA_SAMPLE_HIVE_DDL_TERA = [
    ('amount_value', 'decimal(15,2)', '')]

SAMPLE_HIVE_EXPORT_FALSE = [
    ('login', 'string(32)', '')]

DATA_SAMPLE_HIVE_ROWS = [
    [('testid123', '123424222', 12425255, datetime.datetime(2016, 1, 21, 0, 0),
      '12425255', '01', 'testid123', '',
      datetime.datetime(2010, 2, 16, 13, 22, 38, 637000), 'SSO',
      'test@test.com', 4096, 'N', ' ')],
    [('test252gs', '123453636', 12345353, datetime.datetime(2016, 3, 6, 0, 0),
      '12425255', '01', 'testid123', '',
      datetime.datetime(2016, 1, 31, 5, 1, 15, 650000), 'G2',
      None, 4096, 'Y', ' ')],
    [('testsgshh', '123456789', 12345656, datetime.datetime(2016, 7, 4, 0, 0),
      '12425255', '01', 'testid123', '',
      datetime.datetime(2017, 1, 16, 13, 8, 27, 847000), 'SSO',
      ' ', 4096, 'N', ' ')]
]
