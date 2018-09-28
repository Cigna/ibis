```
 ▄█  ▀█████████▄   ▄█     ▄████████
███    ███    ███ ███    ███    ███
███▌   ███    ███ ███▌   ███    █▀
███▌  ▄███▄▄▄██▀  ███▌   ███
███▌ ▀▀███▀▀▀██▄  ███▌ ▀███████████
███    ███    ██▄ ███           ███
███    ███    ███ ███     ▄█    ███
█▀   ▄█████████▀  █▀    ▄████████▀

```

Version 1.9.4

Python 2.7.8

## usage: COM_TEAM_ibis-1.9.4-py2.7.egg[-h] [--db DB] [--table TABLE]
                                    [--frequency FREQUENCY]

                                    [--teamname TEAMNAME]

                                    [--activate ACTIVATE] --env ENV

                                    [--for-env FOR_ENV] [--checks-balances]

                                    [--submit-request SUBMIT_REQUEST]

                                    [--export-request EXPORT_REQUEST]

                                    [--submit-request-prod SUBMIT_REQUEST_PROD]

                                    [--save-it-table]

                                    [--update-it-table UPDATE_IT_TABLE]

                                    [--update-it-table-export UPDATE_IT_TABLE_EXPORT]

                                    [--run-job RUN_JOB] [--view]

                                    [--view-name VIEW_NAME]

                                    [--select SELECT [SELECT ...]]

                                    [--where WHERE]

                                    [--gen-esp-workflow GEN_ESP_WORKFLOW [GEN_ESP_WORKFLOW ...]]

                                    [--gen-esp-workflow-tables GEN_ESP_WORKFLOW_TABLES]

                                    [--gen-export-esp-workflow-tables GEN_EXPORT_ESP_WORKFLOW_TABLES]

                                    [--data-mask DATA_MASK]

                                    [--gen-config-workflow GEN_CONFIG_WORKFLOW]

                                    [--config-workflow-properties CONFIG_WORKFLOW_PROPERTIES]

                                    [--queue-name QUEUE_NAME]

                                    [--esp-id ESP_ID] [--message MESSAGE]

                                    [--export] [--to TO] [--auth-test]

                                    [--export-oracle] [--source-db SOURCE_DB]

                                    [--source-table SOURCE_TABLE]

                                    [--source-dir SOURCE_DIR]

                                    [--jdbc-url JDBC_URL]

                                    [--target-schema TARGET_SCHEMA]

                                    [--target-table TARGET_TABLE]

                                    [--update-key [UPDATE_KEY [UPDATE_KEY ...]]]

                                    [--user-name USER_NAME]

                                    [--password-file PASSWORD_FILE]

                                    [--pass-alias PASS_ALIAS]

                                    [--source-type SOURCE_TYPE]

                                    [--source-schema SOURCE_SCHEMA]

                                    [--export_teradata]

                                    [--gen-it-table GEN_IT_TABLE]

                                    [--gen-qa-data-sampling GEN_QA_DATA_SAMPLING]

                                    [--parse-request-file PARSE_REQUEST_FILE]

                                    [--hive [HIVE [HIVE ...]]]

                                    [--shell [SHELL [SHELL ...]]]

                                    [--impala [IMPALA [IMPALA ...]]]

                                    [--gen-action [GEN_ACTION [GEN_ACTION ...]]]

                                    [--retrieve-backup] [--update-activator]

                                    [--wipe-perf-env WIPE_PERF_ENV]

                                    [--reingest-all] [--no-git]

                                    [--no-dry-run] 

                                    [--timeout TIMEOUT] [--ingest-version]

                                    [--skip-profile]

                                    [--kite-ingest KITE_INGEST]

                                    [--delete-views [DELETE_VIEWS [DELETE_VIEWS ...]]]

                                    [--db-env DB_ENV] [--operation OPERATION]


## optional arguments:

  -h, --help            show this help message and exit

  --db DB               Used to provide a database name

  --table TABLE         Used to provide a table name

  --frequency FREQUENCY
                        Used to provide a frequency

  --teamname TEAMNAME   Used to provide a team name

  --activate ACTIVATE   Used to provide a activator(yes/no)

  --env ENV             REQUIRED. Used to provide the ibis environment for
                        properties file.

  --for-env FOR_ENV     Optional. To create workflow of dev on prod.

  --checks-balances     Used to interact with check balances table. required:
                        --db {db_name}, --table {tbl_name}options: --update-
                        lifespan list[str], --update-all-lifespan

  --submit-request SUBMIT_REQUEST
                        Used to generate oozie workflow

  --export-request EXPORT_REQUEST
                        Used to generate oozie workflow

  --submit-request-prod SUBMIT_REQUEST_PROD
                        Used to mark changes in it table into staging_it_table

  --save-it-table       Saves all records in it_table to file

  --update-it-table UPDATE_IT_TABLE
                        Used to submit text file containing table properties
                        for the IT table

  --update-it-table-export UPDATE_IT_TABLE_EXPORT
                        Used to submit text file containing table properties
                        for the IT table export

  --run-job RUN_JOB     Used to submit a workflow to run an oozie job

  --view                Create a view. required: --view-name {name}, --db
                        {db_name}, --table {tbl_name} optional param: --select
                        {cols} , --where {statement}

  --view-name VIEW_NAME
                        Used to provide a view name

  --select SELECT [SELECT ...]
                        Used to provide a list of columns

  --where WHERE         Used to provide a where statement

  --gen-esp-workflow GEN_ESP_WORKFLOW [GEN_ESP_WORKFLOW ...]
                        Create workflow(s) based on a list of ESP ids
                        separated by spaces.

  --gen-esp-workflow-tables GEN_ESP_WORKFLOW_TABLES
                        Create workflow(s) based on a list of tables from
                        request file

  --gen-export-esp-workflow-tables GEN_EXPORT_ESP_WORKFLOW_TABLES
                        Create export workflow(s) based on a table from
                        request file

  --data-mask DATA_MASK
                        Create workflow(s) for masked oracle tables, do not
                        provide views in request file

  --gen-config-workflow GEN_CONFIG_WORKFLOW
                        Used to generate custom hive or shell scripts in
                        workflows

  --config-workflow-properties CONFIG_WORKFLOW_PROPERTIES
                        Used to provide config workflow properties

  --queue-name QUEUE_NAME
                        Used for providing hadoop queue name

  --esp-id ESP_ID       esp-appl-id

  --message MESSAGE     Provide description for bmrs

  --export              Export hadoop table to teradata. required: --db {db},
                        name of db you want to export, --table {table}, name
                        of table you want to export, --to {db}.{table}, name
                        of database and table to export to

  --to TO               Used to provide {database}.{table} to export to in
                        Teradata

  --auth-test           Test sqoop authrequired: --source-db {db}, name of db
                        you want to check access,--source-table {table}, name
                        of table you want to check access,--source-schema
                        {schema}, name of schema for sqlserver/postgresql
                        server,--jdbc-url {jdbcurl}, connection string for
                        target schema--user-name {user_name}, db user name
                        --password-file {hdfs_path}, hdfs password file path

  --export-oracle       Export hadoop table to Oracle. required: --source-db
                        {db}, name of db you want to export,--source-table
                        {table}, name of table you want to export,--source-dir
                        {dir}, hdfs location of export table,--jdbc-url
                        {jdbcurl}, connection string for target schema
                        ,--target-schema {targetdb}, oracle schema,--target-
                        table {targettable}, oracle table--update-key
                        {updatekey}, non mandatory param - primary key on
                        target table,--user-name {username}, oracle username
                        ,--pass-alias {passalias}, oracle password alias or
                        jceks url

  --source-db SOURCE_DB
                        Used to provide source hive schema to export to oracle

  --source-table SOURCE_TABLE
                        Used to provide source hive table to export to oracle

  --source-dir SOURCE_DIR
                        Used to provide hdfs source directory to export
                        tooracle, directory should not include the final table
                        directory

  --jdbc-url JDBC_URL   Used to provide oracle connection information to
                        export to oracle

  --target-schema TARGET_SCHEMA
                        Used to provide oracle target schema to export to
                        oracle

  --target-table TARGET_TABLE
                        Used to provide oracle target table to export to
                        oracle

  --update-key [UPDATE_KEY [UPDATE_KEY ...]]
                        Used to provide oracle primary key to export to oracle

  --user-name USER_NAME
                        Used to provide oracle user name to export to oracle

  --password-file PASSWORD_FILE
                        Used to provide oracle password file

  --pass-alias PASS_ALIAS
                        Used to provide oracle password alias to export to
                        oracle

  --source-type SOURCE_TYPE
                        Used to provide source vendor type

  --source-schema SOURCE_SCHEMA
                        Used to provide sql/postgresql schema

  --export_teradata     Export hadoop table to Teradata. required: --source-db
                        {db}, name of db you want to export,--source-table
                        {table}, name of table you want to export,--source-dir
                        {dir}, hdfs location of export table,--jdbc-url
                        {jdbcurl}, connection string for target schema
                        ,--target-schema {targetdb}, teradata Database
                        ,--target-table {targettable}, teradata table--user-
                        name {username}, oracle username,--pass-alias
                        {passalias}, oracle password alias or jceks url

  --gen-it-table GEN_IT_TABLE
                        Generate IT table with automatic split-by if possible

  --gen-qa-data-sampling GEN_QA_DATA_SAMPLING
                        Generate workflow for QA data sampling

  --parse-request-file PARSE_REQUEST_FILE
                        Print each table in request file as json

  --hive [HIVE [HIVE ...]]
                        Generates hive action workflow

  --shell [SHELL [SHELL ...]]
                        Generates shell action workflow

  --impala [IMPALA [IMPALA ...]]
                        Generate impala action workflow

  --gen-action [GEN_ACTION [GEN_ACTION ...]]
                        Generates action for hive,shell,impala in one xml

  --retrieve-backup     Copies backup files to live. required: --db {name}
                        --table {name}

  --update-activator    provide team frequency, Activator(yes/no), team name
                        and full table name

  --wipe-perf-env WIPE_PERF_ENV
                        Provide the team_name or database name for dropping
                        all tables

  --reingest-all        Use this option with wipe-perf-env to reingest all
                        tables

  --no-git              Not saving workflow to git

  --no-dry-run          Dont dry run workflow

  --timeout TIMEOUT     Timeout duration for auto split by

  --ingest-version      Get the ingest version used for the xml

  --skip-profile        Flag to activate podium profiling

  --kite-ingest KITE_INGEST
                        Used to generate kite-ingest workflow

  --delete-views [DELETE_VIEWS [DELETE_VIEWS ...]]
                        Delete views in IT Table

  --db-env DB_ENV       db env - the current env variable

  --operation OPERATION
                        Operation: Update or Delete for Views
