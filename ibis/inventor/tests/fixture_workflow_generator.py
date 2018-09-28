sqls_int = {
    'jdbcurl': 'jdbc:sqlserver://fake.sqlserver:5016;database=fake_database',
    'db_username': 'fake_username', 'password_file': '/user/dev/fake.password.file',
    'weight': 'heavy', 'mappers': 8, 'refresh_frequency': 'none',
    'fetch_size': 50000, 'source_database_name': 'fake_database',
    'source_table_name': 'fake_tablename_sqls_int', 'source_schema_name': 'dbo',
    'split_by': 'fake_split_by'
}
sqls_sys = {
    'jdbcurl': 'jdbc:sqlserver://fake.sqlserver:5016;database=fake_database',
    'db_username': 'fake_username', 'password_file': '/user/dev/fake.password.file',
    'weight': 'heavy', 'mappers': 8, 'refresh_frequency': 'none',
    'fetch_size': 50000, 'source_database_name': 'fake_database',
    'source_table_name': 'fake_tablename_sqls_sys', 'source_schema_name': 'dbo',
    'split_by': 'fake_split_by', 'db_env': 'sys'
}
db2_fake_tablename = {
    'source_table_name': 'db2_fake_tablename',
    'source_database_name': 'fake_database', 'domain': 'fake_database_i',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords'
                     '.jceks#fake.password.alias',
    'db_username': 'fake_username', 'load': '000101', 'check_column': 'TEST_COLUMN',
    'jdbcurl': 'jdbc:db2://fake.db2:50200/fake_servicename',
    'split_by': 'fake_split_by', 'mappers': '10', 'fetch_size': '500'
}
dollar_fake_tablename = {
    'source_table_name': 'fake_$tablename',
    'source_database_name': 'fake_database', 'domain': 'fake_database_i',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#fake.password.alias',
    'db_username': 'fake_username',
    'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
               'fake_servicename',
    'split_by': 'fake_$split_by', 'load': '000101', 'mappers': '10',
    'fetch_size': '50000', 'views': 'pharmacy|claim',
    'source_schema_name': 'dbo', 'check_column': 'TEST_column'
}
sqlserver_fake_tablename = {
    'source_table_name': 'sqlserver_fake_tablename',
    'source_database_name': 'fake_database', 'domain': 'fake_database_i',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#fake.password.alias',
    'db_username': 'fake_username',
    'jdbcurl': 'jdbc:sqlserver://fake.sqlserver:50538;'
               'database=fake_database', 'split_by': 'fake_split_by',
    'load': '000101', 'mappers': '10', 'fetch_size': '50000',
    'views': 'cpm', 'source_schema_name': 'dbo', 'check_column': 'TEST_column'
}
sqlserver_fake_tablename_NON_DBO = {
    'source_table_name': 'sqlserver_fake_tablename',
    'source_database_name': 'fake_database', 'domain': 'fake_database_i',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#fake.password.alias',
    'db_username': 'fake_username',
    'jdbcurl': 'jdbc:sqlserver://fake.sqlserver:50538;'
               'database=fake_database', 'split_by': 'fake_split_by',
    'load': '000101', 'mappers': '10', 'fetch_size': '50000',
    'views': 'cpm', 'source_schema_name': 'fake_warehouse_schema', 'check_column': 'TEST_column'
}
td_fake_tablename = {
    'source_table_name': 'td_fake_tablename', 'db_username': 'fake_username',
    'source_database_name': 'fake_database', 'domain': 'fake_database',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords'
                     '.jceks#fake.password.alias',
    'jdbcurl': 'jdbc:teradata://fake.teradata/database=fake_database',
    'split_by': 'fake_split_by', 'load': '000101', 'mappers': '10',
    'fetch_size': '50000', 'views': 'RXMAIN', 'check_column': 'TEST_COLUMN'
}
fake_fact_tbl_prop = {
    'full_table_name': 'fake_domain.fake_database_risk_fake_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/risk_fake_tablename', 'split_by': '',
    'mappers': 10, 'db_env': 'dev',
    'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
               'fake_servicename',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks'
                     '#fake.password.alias',
    'load': '000010', 'fetch_size': 50000, 'hold': 0, 'views':'fake_view_open',
    'source_database_name': 'fake_database',
    'source_table_name': 'risk_fake_tablename'}
fake_fact_tbl_prop_mysql = {
    'full_table_name': 'fake_database_risk_fake_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/risk_fake_tablename',
    'split_by': 'mysql_split_BY', 'mappers': 10,
    'check_column': 'TEST_INCR_column',
    'jdbcurl': 'jdbc:mysql://fake.mysql/ibis',
    'db_username': 'fake_username',
    'password_file': '/user/dev/fake.password.file',
    'load': '000010', 'fetch_size': 50000, 'hold': 0, 'views':'fake_view_open',
    'source_database_name': 'fake_database',
    'source_table_name': 'risk_fake_tablename'}
fake_ben_tbl_prop = {
    'full_table_name': 'fake_domain.fake_database_fake_ben_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/fake_ben_tablename', 'split_by': '',
    'mappers': 10, 'db_env': 'dev',
    'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521'
               '/fake_servicename',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                     'fake.password.alias',
    'load': '000100', 'fetch_size': 50000, 'hold': 0, 'views':'fake_view_open',
    'source_database_name': 'fake_database',
    'source_table_name': 'fake_ben_tablename'}
fake_ben_tbl_prop_mysql = {
    'full_table_name': 'fake_database_fake_ben_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/fake_ben_tablename',
    'split_by': '', 'mappers': 10,
    'jdbcurl': 'jdbc:mysql://fake.mysql/ibis',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': '/user/dev/fake.password.file',
    'load': '000100', 'fetch_size': 50000, 'hold': 0, 'views':'fake_view_open',
    'source_database_name': 'fake_database',
    'source_table_name': 'fake_ben_tablename'}
fake_ben_pwdfile_tbl_prop = {
    'full_table_name': 'fake_domain.fake_database_fake_ben_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/fake_ben_tablename', 'split_by': '',
    'mappers': 10, 'db_env': 'dev',
    'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521'
               '/fake_servicename',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username', 'password_file': '/user/dev/fake.password.file',
    'load': '000100', 'fetch_size': 50000, 'hold': 0, 'views':'fake_view_open',
    'source_database_name': 'fake_database',
    'source_table_name': 'fake_ben_tablename'}
fake_prof_tbl_prop = {
    'full_table_name': 'fake_domain.fake_database_fake_prof_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/fake_prof_tablename', 'split_by': '',
    'mappers': 10, 'db_env': 'dev',
    'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
               'fake_servicename',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                     'fake.password.alias',
    'load': '000001', 'fetch_size': 50000, 'hold': 0, 'views':'fake_view_open',
    'source_database_name': 'fake_database',
    'source_table_name': 'fake_prof_tablename'}
fake_prof_tbl_prop_mysql = {
    'full_table_name': 'fake_database_fake_prof_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/fake_prof_tablename',
    'split_by': '', 'mappers': 10,
    'jdbcurl': 'jdbc:mysql://fake.mysql/ibis',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': '/user/dev/fake.password.file',
    'load': '000001', 'fetch_size': 50000, 'hold': 0, 'views':'fake_view_open',
    'source_database_name': 'fake_database',
    'source_table_name': 'fake_prof_tablename'}
fake_fct_tbl_prop = {
    'full_table_name': 'fake_domain.fake_database_fake_fct_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/fake_fct_tablename', 'split_by': '',
    'mappers': 10, 'db_env': 'dev',
    'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
               'fake_servicename',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                     'fake.password.alias',
    'load': '000010', 'fetch_size': 50000, 'hold': 0, 'views':'fake_view_open',
    'source_database_name': 'fake_database', 'source_table_name': 'fake_fct_tablename'}
fake_fct_tbl_prop_mysql = {
    'full_table_name': 'fake_database_fake_fct_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/fake_fct_tablename',
    'split_by': '', 'mappers': 10,
    'jdbcurl': 'jdbc:mysql://fake.mysql/ibis',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': '/user/dev/fake.password.file',
    'load': '000010', 'fetch_size': 50000, 'hold': 0, 'views':'fake_view_open',
    'source_database_name': 'fake_database', 'source_table_name': 'fake_fct_tablename'}
fake_cens_tbl_prop = {
    'full_table_name': 'fake_domain.fake_database_fake_cens_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/fake_cens_tablename', 'split_by': '',
    'mappers': 10, 'db_env': 'dev',
    'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
               'fake_servicename',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                     'fake.password.alias',
    'load': '000100', 'fetch_size': 50000, 'hold': 0, 'views':'fake_view_open',
    'source_database_name': 'fake_database',
    'source_table_name': 'fake_cens_tablename'}
fake_cens_tbl_prop_mysql = {
    'full_table_name': 'fake_database_fake_cens_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/fake_cens_tablename',
    'split_by': '', 'mappers': 10,
    'jdbcurl': 'jdbc:mysql://fake.mysql/ibis',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': '/user/dev/fake.password.file',
    'load': '000100', 'fetch_size': 50000, 'hold': 0, 'views':'fake_view_open',
    'source_database_name': 'fake_database',
    'source_table_name': 'fake_cens_tablename'}
light_3_prop = {
    'full_table_name': 'fake_domain.fake_database_fake_cens_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/fake_cens_tablename', 'split_by': '',
    'mappers': 10, 'db_env': 'dev',
    'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
               'fake_servicename',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                     'fake.password.alias',
    'load': '000100', 'fetch_size': 50000, 'hold': 0, 'views':'fake_view_open',
    'source_database_name': 'fake_database', 'source_table_name': 'light_3'}
light_3_prop_mysql = {
    'full_table_name': 'fake_database_fake_cens_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/fake_cens_tablename', 'split_by': '',
    'mappers': 10,
    'jdbcurl': 'jdbc:mysql://fake.mysql/ibis',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': '/user/dev/fake.password.file',
    'load': '000100', 'fetch_size': 50000, 'hold': 0, 'views':'fake_view_open',
    'source_database_name': 'fake_database', 'source_table_name': 'light_3'}
light_4_prop = {
    'full_table_name': 'fake_domain.fake_database_fake_cens_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/fake_cens_tablename', 'split_by': '',
    'mappers': 10, 'db_env': 'dev',
    'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521'
               '/fake_servicename',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                     'fake.password.alias',
    'load': '000100', 'fetch_size': 50000, 'hold': 0,
    'source_database_name': 'fake_database', 'source_table_name': 'light_4'}
light_4_prop_mysql = {
    'full_table_name': 'fake_database_fake_cens_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/fake_cens_tablename',
    'split_by': '', 'mappers': 10,
    'jdbcurl': 'jdbc:mysql://fake.mysql/ibis',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': '/user/dev/fake.password.file',
    'load': '000100', 'fetch_size': 50000, 'hold': 0,
    'source_database_name': 'fake_database', 'source_table_name': 'light_4'}
light_5_prop = {
    'full_table_name': 'fake_domain.fake_database_fake_cens_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/fake_cens_tablename', 'split_by': '',
    'mappers': 10, 'db_env': 'dev',
    'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
               'fake_servicename',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                     'fake.password.alias',
    'load': '000100', 'fetch_size': 50000, 'hold': 0, 'views':'fake_view_open',
    'source_database_name': 'fake_database', 'source_table_name': 'light_5'}
med_3_prop = {
    'full_table_name': 'fake_domain.fake_database_fake_fct_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/fake_fct_tablename', 'split_by': '',
    'mappers': 10, 'db_env': 'dev',
    'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
               'fake_servicename',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                     'fake.password.alias',
    'load': '000010', 'fetch_size': 50000, 'hold': 0, 'views':'fake_view_open',
    'source_database_name': 'fake_database', 'source_table_name': 'med_3'}
med_3_prop_mysql = {
    'full_table_name': 'fake_database_fake_fct_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/fake_fct_tablename',
    'split_by': '', 'mappers': 10,
    'jdbcurl': 'jdbc:mysql://fake.mysql/ibis',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': '/user/dev/fake.password.file',
    'load': '000010', 'fetch_size': 50000, 'hold': 0, 'views':'fake_view_open',
    'source_database_name': 'fake_database', 'source_table_name': 'med_3'}
heavy_2_prop = {
    'full_table_name': 'fake_domain.fake_database_fake_prof_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/fake_prof_tablename', 'split_by': '',
    'mappers': 10, 'db_env': 'dev',
    'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
               'fake_servicename',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                     'fake.password.alias',
    'load': '000001', 'fetch_size': 50000, 'hold': 0, 'views':'fake_view_open',
    'source_database_name': 'fake_database', 'source_table_name': 'heavy_2',
    'esp_appl_id': 'test_esp_appl_id_2', 'esp_group': '',
    'check_column': 'test_incr_column'}
heavy_2_prop_mysql = {
    'full_table_name': 'fake_database_fake_prof_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/fake_prof_tablename',
    'split_by': '', 'mappers': 10,
    'jdbcurl': 'jdbc:mysql://fake.mysql/ibis',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': '/user/dev/fake.password.file',
    'load': '000001', 'fetch_size': 50000, 'hold': 0, 'views':'fake_view_open',
    'source_database_name': 'fake_database', 'source_table_name': 'heavy_2',
    'esp_appl_id': 'test_esp_appl_id_2', 'esp_group': '',
    'check_column': 'test_incr_column'}
heavy_3_prop = {
    'full_table_name': 'fake_domain.fake_database_fake_prof_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/fake_prof_tablename', 'split_by': '',
    'mappers': 10, 'db_env': 'dev',
    'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
               'fake_servicename',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                     'fake.password.alias',
    'load': '000001', 'fetch_size': 50000, 'hold': 0, 'views':'fake_view_open',
    'source_database_name': 'fake_database', 'source_table_name': 'heavy_3',
    'esp_appl_id': 'test_esp_appl_id', 'esp_group': '',
    'check_column': 'test_incr_column'}
heavy_3_prop_perf = {
    'full_table_name': 'fake_domain.fake_database_fake_prof_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/fake_prof_tablename', 'split_by': '',
    'mappers': 10, 'db_env': 'dev',
    'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
               'fake_servicename',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                     'fake.password.alias',
    'load': '000001', 'fetch_size': 50000, 'hold': 0, 'views':'fake_view_open',
    'source_database_name': 'fake_database', 'source_table_name': 'heavy_3',
    'esp_appl_id': 'test_esp_appl_id', 'esp_group': '',
    'check_column': 'test_incr_column'}
heavy_3_prop_exp = {
    'full_table_name': 'fake_domain.fake_database_fake_prof_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/fake_prof_tablename', 'split_by': '',
    'mappers': 10,
    'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
               'fake_servicename',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                     'fake.password.alias',
    'load': '000001', 'fetch_size': 50000, 'hold': 0, 'views':'fake_view_open',
    'source_database_name': 'fake_database', 'source_table_name': 'heavy_3',
    'esp_appl_id': 'test_esp_appl_id', 'esp_group': '',
    'check_column': 'test_incr_column', 'target_schema': 'fake_database',
    'database_name': 'fake_database', 'table_name': 'heavy_3',
    'target_table': 'heavy_3'}
heavy_3_prop_mysql = {
    'full_table_name': 'fake_database_fake_prof_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/fake_prof_tablename',
    'split_by': '', 'mappers': 10,
    'jdbcurl': 'jdbc:mysql://fake.mysql/ibis',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': '/user/dev/fake.password.file',
    'load': '000001', 'fetch_size': 50000, 'hold': 0, 'views':'fake_view_open',
    'source_database_name': 'fake_database', 'source_table_name': 'heavy_3',
    'esp_appl_id': 'test_esp_appl_id', 'esp_group': '',
    'check_column': 'test_incr_column'}

full_ingest_tbl_mysql = {
    'domain': 'member',
    'jdbcurl': 'mysql://fake.mysql/ibis',
    'db_username': 'fake_username',
    'password_file': '/user/dev/fake.password.file',
    'load': '000001', 'fetch_size': 50000, 'hold': 0, 'views': 'fake_view_im',
    'source_database_name': 'fake_database', 'source_table_name': 'fake_mem_tablename'}

full_ingest_tbl = {
    'domain': 'member', 'db_env': 'dev',
    'target_dir': 'mdm/member/fake_database/fake_mem_tablename',
    'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
               'fake_servicename',
    'db_username': 'fake_username',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                     'fake.password.alias',
    'load': '000001', 'fetch_size': 50000, 'hold': 0, 'views': 'fake_view_im',
    'source_database_name': 'fake_database', 'source_table_name': 'fake_mem_tablename'}

full_ingest_tbl_custom_config = {
    'domain': 'member', 'db_username': 'fake_username', 'db_env': 'dev',
    'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
               'fake_servicename',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                     'fake.password.alias',
    'load': '000001', 'fetch_size': 50000, 'hold': 0, 'views': 'fake_view_im',
    'actions': 'custom_config_no_views.dsl',
    'source_database_name': 'fake_database', 'source_table_name': 'fake_mem_tablename'}

fake_algnmt_tbl = {
    'domain': 'member',
    'target_dir': 'mdm/member/fake_database/fake_algnmt_tablename',
    'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
               'fake_servicename',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                     'fake.password.alias',
    'load': '000001', 'fetch_size': 50000, 'hold': 0, 'views': 'fake_view_im',
    'source_database_name': 'fake_database', 'source_table_name': 'fake_algnmt_tablename'}

full_ingest_tbl_custom_config_req = {
    'source_database_name': 'fake_database', 'source_table_name': 'fake_mem_tablename'
}

light_req_1 = {'source_database_name': 'fake_database1',
               'source_table_name': 'fake_cens_tablename',
               'refresh_frequency': 'weekly'}
light_req_2 = {'source_database_name': 'fake_database2',
               'source_table_name': 'fake_ben_tablename',
               'refresh_frequency': 'weekly'}
light_req_3 = {'source_database_name': 'fake_database',
               'source_table_name': 'light_3',
               'refresh_frequency': 'weekly'}
light_req_4 = {'source_database_name': 'fake_database',
               'source_table_name': 'light_4',
               'refresh_frequency': 'weekly'}
light_req_5 = {'source_database_name': 'fake_database',
               'source_table_name': 'light_5',
               'refresh_frequency': 'weekly'}
med_req_1 = {'source_database_name': 'fake_database',
             'source_table_name': 'fake_risk_tablename',
             'refresh_frequency': 'weekly'}
med_req_2 = {'source_database_name': 'fake_database',
             'source_table_name': 'fake_fct_tablename',
             'refresh_frequency': 'weekly'}
med_req_3 = {'source_database_name': 'fake_database',
             'source_table_name': 'med_3',
             'refresh_frequency': 'weekly'}
heavy_req_1 = {'source_database_name': 'fake_database',
               'source_table_name': 'fake_prof_tablename',
               'refresh_frequency': 'weekly'}
heavy_req_2 = {'source_database_name': 'fake_database',
               'source_table_name': 'heavy_2',
               'refresh_frequency': 'weekly'}
heavy_req_3 = {'source_database_name': 'fake_database',
               'source_table_name': 'heavy_3',
               'refresh_frequency': 'weekly'}

mock_table_mapping_val = {'load': '000010', 'mappers': 10,
                          'domain': 'member', 'source_database_name': 'fake_database',
                          'source_table_name': 'fake_mmm_tablename',
                          'connection_factories': 'com.cloudera.sqoop.manager.'
                                                  'DefaultManagerFactory',
                          'password_file': 'jceks://hdfs/user/dev/'
                                           'fake.passwords.jceks#fake.password.alias',
                          'full_table_name': 'fake_database.fake_mmm_tablename',
                          'db_username': 'fake_username', 'hold': 0,
                          'split_by': '', 'jdbcurl': 'oracle',
                          'fetch_size': 5, 'esp_appl_id': 'TEST01'}

mock_table_mapping_val_export = {'source_database_name': 'test_mysql',
                                 'source_table_name': 'test_data',
                                 'jdbcurl': 'oracle', 'target_schema': 'dvi',
                                 'target_table': 'ogd', 'db_username': 'ise',
                                 'password_file': 'jceks://hdfs/user/dev/'
                                                  'fake.passwords.jceks#fake.password.alias'}

mock_table_mapping_val_app = {
    'load': '000010', 'mappers': 10, 'domain': 'member',
    'source_database_name': 'fake_database', 'source_table_name': 'fake_mmm_tablename',
    'connection_factories': 'com.cloudera.sqoop.manager.DefaultManagerFactory',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#fake.password.alias',
    'full_table_name': 'fake_database.fake_mmm_tablename', 'db_username': 'fake_username', 'hold': 0,
    'split_by': '', 'jdbcurl': 'oracle', 'fetch_size': 5, 'esp_appl_id': ''}

mock_table_mapping_val_exp = {
    'full_table_name': 'fake_domain.fake_database_fake_prof_tablename',
    'source_dir': 'mdm/test/fake_database/fake_prof_tablename',
    'mappers': 10,
    'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
               'fake_servicename',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                     'fake.password.alias',
    'frequency': 'daily', 'fetch_size': 50000, 'target_schema': 'fake_database',
    'target_table': 'heavy_3', 'source_database_name': 'fake_database',
    'source_table_name': 'heavy_3'}

heavy_3_prop_exp = {
    'full_table_name': 'fake_domain.fake_database_fake_prof_tablename', 'domain': 'test',
    'target_dir': 'mdm/test/fake_database/fake_prof_tablename', 'split_by': '',
    'mappers': 10,
    'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
               'fake_servicename',
    'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
    'db_username': 'fake_username',
    'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                     'fake.password.alias',
    'load': '000001', 'fetch_size': 50000, 'hold': 0, 'views':'fake_view_open',
    'source_database_name': 'fake_database', 'source_table_name': 'heavy_3',
    'esp_appl_id': 'test_esp_appl_id', 'esp_group': '',
    'check_column': 'test_incr_column', 'target_schema': 'fake_database',
    'database_name': 'fake_database', 'table_name': 'heavy_3',
    'target_table': 'heavy_3'}

mock_esp_tables_01 = [
    {'load': '000100', 'mappers': 2, 'domain': 'member',
     'target_dir': 'mdm/member/fake_database/fake_ref_tablename',
     'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks'
                      '#fake.password.alias',
     'source_table_name': 'fake_ref_tablename', 'hold': 0, 'split_by': '',
     'fetch_size': 20000, 'esp_appl_id': 'FAKED001',
     'source_database_name': 'fake_database',
     'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
     'full_table_name': 'member.fake_database_fake_ref_tablename',
     'db_username': 'fake_username',
     'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
                'fake_servicename', 'views': 'fake_view_im|client'},
    {'load': '010010', 'mappers': 50, 'domain': 'member',
     'target_dir': 'mdm/member/fake_database/fake_biometrics_tablename',
     'password_file': 'jceks://hdfs/user/dev/fake.passwords.'
                      'jceks#fake.password.alias',
     'source_table_name': 'fake_biometrics_tablename',
     'hold': 0, 'split_by': '', 'fetch_size': 10000, 'esp_appl_id': 'FAKED001',
     'source_database_name': 'fake_database',
     'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
     'full_table_name': 'member.fake_database_fake_biometrics_tablename',
     'db_username': 'fake_username',
     'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/fake_servicename', 'views': 'fake_view_im|client'},
    {'load': '100100', 'mappers': 10, 'domain': 'member',
     'target_dir': 'mdm/member/fake_database/fake_ben_tablename',
     'password_file': 'jceks://hdfs/user/dev/sqoop.'
                      'passwords.jceks#fake.password.alias',
     'source_table_name': 'fake_ben_tablename', 'hold': 0,
     'split_by': '', 'fetch_size': 50000,
     'esp_appl_id': 'FAKED001', 'source_database_name': 'fake_database',
     'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
     'full_table_name': 'member.fake_database_fake_ben_tablename',
     'db_username': 'fake_username',
     'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/fake_servicename', 'views': 'fake_view_im|client'},
    {'load': '000010', 'mappers': 50, 'domain': 'member',
     'target_dir': 'mdm/member/fake_database/fake_assmt_tablename',
     'password_file': 'jceks://hdfs/user/dev/fake.passwords.'
                      'jceks#fake.password.alias',
     'source_table_name': 'fake_assmt_tablename', 'hold': 0,
     'split_by': '', 'fetch_size': 50000,
     'esp_appl_id': 'FAKED001',
     'source_database_name': 'fake_database',
     'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
     'full_table_name': 'member.fake_database_fake_assmt_tablename',
     'db_username': 'fake_username',
     'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/fake_servicename',
     'views': 'fake_view_im|client'},
    {'load': '100100', 'mappers': 50, 'domain': 'member',
     'target_dir': 'mdm/member/fake_database/fake_rule_tablename',
     'password_file': 'jceks://hdfs/user/dev/fake.passwords.'
                      'jceks#fake.password.alias',
     'source_table_name': 'fake_rule_tablename', 'hold': 0,
     'split_by': '', 'fetch_size': 50000, 'esp_appl_id': 'FAKED001',
     'source_database_name': 'fake_database',
     'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
     'full_table_name': 'member.fake_database_fake_rule_tablename', 'db_username': 'fake_username',
     'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/'
                'fake_servicename', 'views': 'fake_view_im|client'},
    {'load': '001001', 'mappers': 50, 'domain': 'member',
     'target_dir': 'mdm/member/fake_database/fake_posiv_tablename',
     'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                      'fake.password.alias',
     'source_table_name': 'fake_posiv_tablename',
     'hold': 0, 'split_by': 'fake_split_by', 'fetch_size': 50000,
     'esp_appl_id': 'FAKED001', 'source_database_name': 'fake_database',
     'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
     'full_table_name': 'member.fake_database_fake_posiv_tablename',
     'db_username': 'fake_username',
     'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/fake_servicename', 'views': 'fake_view_im|client'}]

mock_esp_tables_02 = [
    {'load': '000100', 'mappers': 2, 'domain': 'fake_domain',
     'target_dir': 'mdm/fake_domain/fake_database/fake_prog_tablename',
     'password_file': 'jceks://hdfs/user/dev/fake.passwords.'
                      'jceks#fake.password.alias',
     'source_table_name': 'fake_prog_tablename',
     'hold': 0, 'split_by': '', 'fetch_size': 20000,
     'esp_appl_id': 'FAKED006',
     'source_database_name': 'fake_database',
     'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
     'full_table_name': 'fake_domain.fake_database_fake_prog_tablename',
     'db_username': 'fake_username',
     'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/fake_servicename',
     'views': 'fake_view_open|client'},
    {'load': '100001', 'mappers': 2, 'domain': 'member',
     'target_dir': 'mdm/member/fake_database/fake_cens_tablename',
     'password_file': 'jceks://hdfs/user/dev/fake.passwords'
                      '.jceks#fake.password.alias',
     'source_table_name': 'fake_cens_tablename',
     'hold': 0, 'split_by': '', 'fetch_size': 50000,
     'esp_appl_id': 'FAKED006',
     'source_database_name': 'fake_database',
     'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
     'full_table_name': 'member.fake_database_fake_cens_tablename',
     'db_username': 'fake_username',
     'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/fake_servicename',
     'views': 'fake_view_open|client'},
    {'load': '100010', 'mappers': 10, 'domain': 'fake_domain',
     'target_dir': 'mdm/fake_domain/fake_database/risk_fake_tablename',
     'password_file': 'jceks://hdfs/user/dev/fake.passwords.'
                      'jceks#fake.password.alias',
     'source_table_name': 'risk_fake_tablename',
     'hold': 0, 'split_by': '', 'fetch_size': 50000,
     'esp_appl_id': 'FAKED006',
     'source_database_name': 'fake_database',
     'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
     'full_table_name': 'fake_domain.fake_database_risk_fake_tablename',
     'db_username': 'fake_username',
     'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/fake_servicename', 'views': 'fake_view_open|client'}]

mock_esp_tbl_perf_domain = [
    {'load': '000100', 'mappers': 2, 'domain': 'fake_domain',
     'target_dir': 'mdm/fake_domain/fake_database/fake_prog_tablename',
     'password_file': 'jceks://hdfs/user/dev/fake.passwords.'
                      'jceks#fake.password.alias',
     'source_table_name': 'fake_prog_tablename',
     'hold': 0, 'split_by': '', 'fetch_size': 20000,
     'esp_appl_id': 'FAKED006',
     'source_database_name': 'fake_database',
     'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
     'full_table_name': 'fake_domain.fake_database_fake_prog_tablename',
     'db_username': 'fake_username',
     'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/fake_servicename', 'views':'fake_view_open'},
    {'load': '100001', 'mappers': 2, 'domain': 'member',
     'target_dir': 'mdm/member/fake_database/fake_cens_tablename',
     'password_file': 'jceks://hdfs/user/dev/fake.passwords'
                      '.jceks#fake.password.alias',
     'source_table_name': 'fake_cens_tablename',
     'hold': 0, 'split_by': '', 'fetch_size': 50000,
     'esp_appl_id': 'FAKED006',
     'source_database_name': 'fake_database',
     'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
     'full_table_name': 'member.fake_database_fake_cens_tablename',
     'db_username': 'fake_username',
     'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/fake_servicename',
     'views':'fake_view_open'},
    {'load': '100010', 'mappers': 10, 'domain': 'fake_domain',
     'target_dir': 'mdm/fake_domain/fake_database/risk_fake_tablename',
     'password_file': 'jceks://hdfs/user/dev/fake.passwords.'
                      'jceks#fake.password.alias',
     'source_table_name': 'risk_fake_tablename',
     'hold': 0, 'split_by': '', 'fetch_size': 50000,
     'esp_appl_id': 'FAKED006',
     'source_database_name': 'fake_database',
     'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
     'full_table_name': 'fake_domain.fake_database_risk_fake_tablename',
     'db_username': 'fake_username',
     'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/fake_servicename', 'views':'fake_view_open'}]

mock_esp_tables_03 = [
    {'load': '000010', 'mappers': 2, 'domain': 'fake_domain',
     'target_dir': 'mdm/fake_domain/fake_database/fake_prog_tablename',
     'password_file': 'jceks://hdfs/user/dev/fake.passwords.'
                      'jceks#fake.password.alias',
     'source_table_name': 'fake_prog_tablename',
     'hold': 0, 'split_by': '', 'fetch_size': 20000,
     'esp_appl_id': 'FAKED006',
     'source_database_name': 'fake_database',
     'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
     'full_table_name': 'fake_domain.fake_database_fake_prog_tablename',
     'db_username': 'fake_username',
     'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/fake_servicename',
     'views': 'fake_view_open|client'},
    {'load': '100100', 'mappers': 2, 'domain': 'member',
     'target_dir': 'mdm/member/fake_database/fake_cens_tablename',
     'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#dev.'
                      'fake.password.alias',
     'source_table_name': 'fake_cens_tablename',
     'hold': 0, 'split_by': '', 'fetch_size': 50000,
     'esp_appl_id': 'FAKED006',
     'source_database_name': 'fake_database',
     'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
     'full_table_name': 'member.fake_database_fake_cens_tablename',
     'db_username': 'fake_username',
     'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/fake_servicename',
     'views': 'fake_view_open|client'},
    {'load': '100001', 'mappers': 10, 'domain': 'fake_domain',
     'target_dir': 'mdm/fake_domain/fake_database/risk_fake_tablename',
     'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                      'fake.password.alias',
     'source_table_name': 'risk_fake_tablename',
     'hold': 0, 'split_by': '', 'fetch_size': 50000,
     'esp_appl_id': 'FAKED006',
     'source_database_name': 'fake_database',
     'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
     'full_table_name': 'fake_domain.fake_database_risk_fake_tablename',
     'db_username': 'fake_username',
     'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/fake_servicename',
     'views': 'fake_view_open|client'},
    {'load': '100001', 'mappers': 10, 'domain': 'fake_domain',
     'target_dir': 'mdm/fake_domain/fake_database/fake_risk_tablename_2',
     'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                      'fake.password.alias',
     'source_table_name': 'fake_risk_tablename_2',
     'hold': 0, 'split_by': '', 'fetch_size': 50000,
     'esp_appl_id': 'FAKED006',
     'source_database_name': 'fake_database',
     'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
     'full_table_name': 'fake_domain.fake_database_fake_risk_tablename_2',
     'db_username': 'fake_username',
     'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/fake_servicename', 'views': 'fake_view_open|client'},
    {'load': '100001', 'mappers': 10, 'domain': 'fake_domain',
     'target_dir': 'mdm/fake_domain/fake_database/fake_risk_tablename_3',
     'password_file': 'jceks://hdfs/user/dev/fake.passwords.jceks#'
                      'fake.password.alias',
     'source_table_name': 'fake_risk_tablename_3',
     'hold': 0, 'split_by': '', 'fetch_size': 50000,
     'esp_appl_id': 'FAKED006',
     'source_database_name': 'fake_database',
     'connection_factories': 'com.quest.oraoop.OraOopManagerFactory',
     'full_table_name': 'fake_domain.fake_database_fake_risk_tablename_3',
     'db_username': 'fake_username',
     'jdbcurl': 'jdbc:oracle:thin:@//fake.oracle:1521/fake_servicename', 'views': 'fake_view_open|client',
     'check_column': 'test_incr_column'}]


appl_ref_id_tbl_01 = [{'job_name': 'C1_FAKE_CALL_FAKE_DATABASE_DAILY',
                       'frequency': 'Daily',
                       'time': '6:00', 'string_date': 'Every Day',
                       'ksh_name': 'call_fake_database_daily', 'domain': 'call',
                       'db': 'fake_database', 'environment': 'DEV'}]

appl_ref_id_tbl_02 = [{'job_name': 'C1_FAKE_CALL2_FAKE_DATABASE_DAILY',
                       'frequency': 'Daily',
                       'time': '6:00', 'string_date': 'Every Day',
                       'ksh_name': 'call_fake_database_daily', 'domain': 'call2',
                       'db': 'fake_database', 'environment': 'DEV'}]

appl_ref_sch_01 = [{'appl_id': 'FAKED001',
                    'esp_name': 'C1_FAKE_FAKE_DATABASE_DAILY',
                    'time': '3:00', 'string_date':
                    'Every Day', 'frequency': 'Daily',
                    'ksh_name': 'call_fake_database_daily.ksh',
                    'priority': 'C1', 'esp_domain': 'call', }]

appl_ref_sch_02 = [{'appl_id': 'FAKED006',
                    'esp_name': 'C1_FAKE_FAKE_DATABASE_DAILY', 'time': '4:00',
                    'string_date': 'Every Day', 'frequency': 'Daily',
                    'ksh_name': 'call_fake_database_daily.ksh',
                    'priority': 'C1', 'esp_domain': 'call', }]

full_ingest_tbl_auto_domain = {
    'target_dir': 'mdm/member/fake_database/fake_mem_tablename',
    'jdbcurl': 'mysql://fake.mysql/ibis',
    'db_username': 'fake_username',
    'password_file': '/user/dev/fake.password.file',
    'load': '000001', 'fetch_size': 50000, 'hold': 0, 'views': 'fake_view_im',
    'source_database_name': 'fake_database', 'source_table_name': 'fake_mem_tablename'}

full_ingest_tbl_auto_domain_env = {
    'target_dir': 'mdm/member/fake_database/fake_mem_tablename',
    'jdbcurl': 'mysql://fake.mysql/ibis',
    'db_username': 'fake_username',
    'db_env': 'int_test',
    'password_file': '/user/dev/fake.password.file',
    'load': '000001', 'fetch_size': 50000, 'hold': 0, 'views': 'fake_view_im',
    'source_database_name': 'fake_database', 'source_table_name': 'fake_mem_tablename'}
