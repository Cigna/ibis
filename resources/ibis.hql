-- Creates required tables for ibis workflow generation and ingestion

-- ibis database/schema
create database if not exists ibis;

-- checks_balances
CREATE EXTERNAL TABLE if not exists ibis.checks_balances (   directory STRING,    pull_time INT,    avro_size BIGINT,    ingest_timestamp STRING,    parquet_time INT,    parquet_size BIGINT,    rows BIGINT,    lifespan STRING,    ack INT,    cleaned INT,    current_repull INT,    esp_appl_id STRING ) PARTITIONED BY (   domain STRING,    table STRING ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION 'hdfs://nameservice1/user/dev/data/ibis/checks_balances';

-- checks_balances_audit
CREATE TABLE if not exists ibis.checks_balances_audit (   directory STRING,    pull_time INT,    avro_size BIGINT,    ingest_timestamp STRING,    parquet_time INT,    parquet_size BIGINT,    rows STRING,    lifespan STRING,    ack INT,    cleaned INT,    domain STRING,    table STRING ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION 'hdfs://nameservice1/user/dev/data/ibis/checks_balances_audit';

-- esp_ids
CREATE TABLE IF NOT EXISTS ibis.esp_ids ( appl_id STRING, job_name STRING, time STRING,    string_date STRING, ksh_name STRING, esp_domain STRING ) PARTITIONED BY (domain STRING, db STRING,  frequency STRING, esp_group STRING, environment STRING) STORED AS PARQUET LOCATION 'hdfs://nameservice1/user/dev/data/ibis/espids';


-- it_table
CREATE TABLE IF NOT EXISTS ibis.it_table (full_table_name STRING,    domain STRING,    target_dir STRING,    split_by STRING,    mappers INT,    jdbcurl STRING,    connection_factories STRING,    db_username STRING,    password_file STRING,    load STRING,    fetch_size INT,    hold INT,    esp_appl_id STRING,    views STRING,    esp_group STRING,    check_column STRING, source_schema_name STRING, sql_query STRING, actions STRING) PARTITIONED BY (   source_database_name STRING,    source_table_name STRING,    db_env STRING ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE LOCATION 'hdfs://nameservice1/user/dev/data/ibis/it_table';

-- table for logging qa action results
CREATE TABLE IF NOT EXISTS ibis.qa_resultsv2 (log_time TIMESTAMP, table_name STRING, log STRING, status STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'  STORED AS TEXTFILE LOCATION 'hdfs://nameservice1/user/hive/warehouse/ibis.db/qa_resultsv2';

-- dev_it_table_export : table for logging export table entry
CREATE TABLE IF NOT EXISTS ibis.dev_it_table_export ( full_table_name STRING,source_dir STRING, mappers INT, jdbcurl STRING, connection_factories STRING, db_username STRING, password_file STRING,  frequency STRING,  fetch_size INT, target_schema STRING,  target_table STRING, staging_database STRING  )   PARTITIONED BY (  source_database_name STRING, source_table_name STRING, db_env    STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE LOCATION 'hdfs://nameservice1/user/dev/data/ibis/dev_it_table_export';

-- export_checks_balances
CREATE TABLE IF NOT EXISTS ibis.checks_balances_export ( directory STRING, push_time INT, txt_size BIGINT, export_timestamp STRING, parquet_time INT, parquet_size BIGINT, row_count BIGINT, lifespan STRING, ack INT, cleaned INT, current_repull INT, esp_appl_id STRING   )   PARTITIONED BY ( domain STRING, table_name STRING  )   ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'   STORED AS TEXTFILE  LOCATION 'hdfs://nameservice1/user/dev/data/ibis/checks_balances_export';

-- table for logging qa action results for export
CREATE TABLE IF NOT EXISTS ibis.qa_export_results ( log_time TIMESTAMP,  table_name STRING, log STRING, status STRING  ) STORED AS TEXTFILE  LOCATION 'hdfs://nameservice1/user/hive/warehouse/ibis.db/qa_export_results';

--table for freq_ingest
CREATE TABLE IF NOT EXISTS `ibis.freq_ingest`(   `frequency` string,   `activate` string) PARTITIONED BY (   `view_name` string,   `full_table_name` string)  STORED AS TEXTFILE  LOCATION 'hdfs://nameservice1/user/dev/data/ibis/freq_ingest';

-- table for teradata split_by replace {0} with servername
CREATE TABLE IF NOT EXISTS ibis.teradata_split_server1 (   databasename STRING,   tablename STRING,   indexnumber SMALLINT,   indextype CHAR(1),   uniqueflag CHAR(1),   columnname STRING,   columnposition SMALLINT,   columntyped STRING,   nullable CHAR(1),   rank1 STRING ) STORED AS TEXTFILE LOCATION 'hdfs://nameservice1/user/hive/warehouse/ibis.db/teradata_split_server1' TBLPROPERTIES ('totalSize'='136662', 'numRows'='2191', 'rawDataSize'='134471', 'COLUMN_STATS_ACCURATE'='true', 'numFiles'='1');
CREATE TABLE IF NOT EXISTS ibis.teradata_split_server2 (   databasename STRING,   tablename STRING,   indexnumber SMALLINT,   indextype CHAR(1),   uniqueflag CHAR(1),   columnname STRING,   columnposition SMALLINT,   columntyped STRING,   nullable CHAR(1),   rank1 STRING ) STORED AS TEXTFILE LOCATION 'hdfs://nameservice1/user/hive/warehouse/ibis.db/teradata_split_server2' TBLPROPERTIES ('totalSize'='136662', 'numRows'='2191', 'rawDataSize'='134471', 'COLUMN_STATS_ACCURATE'='true', 'numFiles'='1');


