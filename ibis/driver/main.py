"""Main module which calls the functionality handler methods."""
import os
import sys
import traceback
from argparse import ArgumentParser, FileType
from ibis.custom_logging import get_logger
from ibis.driver.driver import Driver
from ibis.utilities.config_manager import ConfigManager
from ibis.inventory import inventory
from ibis.utilities.utilities import Utilities

driver = None
logger = None


def main():
    """Command line arguments parser.
    Calls the appropriate handler method
    """
    global driver
    global logger

    parser = ArgumentParser()

    # Properties
    parser.add_argument('--db', nargs=1, type=str,
                        help='Used to provide a database name')
    parser.add_argument('--table', nargs=1, type=str,
                        help='Used to provide a table name')
    parser.add_argument('--frequency', nargs=1, type=str,
                        help='Used to provide a frequency')
    parser.add_argument('--teamname', nargs=1, type=str,
                        help='Used to provide a team name')
    parser.add_argument('--activate', nargs=1, type=str,
                        help='Used to provide a activator(yes/no)')
    parser.add_argument('--env', nargs=1, type=str, required=True,
                        help='REQUIRED. Used to provide the ibis '
                             'environment for properties file.')
    parser.add_argument('--for-env', nargs=1, type=str,
                        help='Optional. To create workflow of dev on prod.')
    # Checks and Balances
    parser.add_argument('--checks-balances', action='store_true',
                        help='Used to interact with check balances table. '
                             'required: --db {db_name}, --table {tbl_name}'
                             'options: --update-lifespan list[str], '
                             '--update-all-lifespan')

    # Business Table
    parser.add_argument('--submit-request', type=FileType('r'),
                        help='Used to generate oozie workflow')
    parser.add_argument('--export-request', type=FileType('r'),
                        help='Used to generate oozie workflow')
    parser.add_argument('--submit-request-prod', type=FileType('r'),
                        help='Used to mark changes in it table '
                             'into staging_it_table')

    # IT Table
    parser.add_argument('--save-it-table', action='store_true',
                        help='Saves all records in it_table to file')

    parser.add_argument('--update-it-table', type=FileType('r'),
                        help='Used to submit text file containing table '
                             'properties for the IT table')

    # IT Table Export
    parser.add_argument('--update-it-table-export', type=FileType('r'),
                        help='Used to submit text file containing table '
                             'properties for the IT table export')

    # Run
    parser.add_argument('--run-job', type=str,
                        help='Used to submit a workflow to run an oozie job')

    # View generation
    parser.add_argument('--view', action='store_true',
                        help='Create a view. required: --view-name '
                             '{name}, --db {db_name}, '
                             '--table {tbl_name} optional param: '
                             '--select {cols} ,'
                             ' --where {statement}')
    parser.add_argument('--view-name', nargs=1, type=str,
                        help='Used to provide a view name')
    parser.add_argument('--select', nargs='+', type=str,
                        help='Used to provide a list of columns')
    parser.add_argument('--where', nargs=1, type=str,
                        help='Used to provide a where statement')

    # Generate workflows base on filter
    parser.add_argument('--gen-esp-workflow', nargs='+', type=str,
                        help='Create workflow(s) based on a list of ESP '
                             'ids separated by spaces.')
    parser.add_argument('--gen-esp-workflow-tables', type=FileType('r'),
                        help='Create workflow(s) based on a list of '
                             'tables from request file')

    # config based workflows
    parser.add_argument('--gen-config-workflow', nargs=1, type=FileType('r'),
                        help='Used to generate custom hive or'
                             ' shell scripts in workflows')
    parser.add_argument('--config-workflow-properties', nargs=1, type=str,
                        help='Used to provide config workflow properties')
    parser.add_argument('--queue-name', nargs=1, type=str,
                        help='Used for providing hadoop queue name')

    parser.add_argument('--esp-id', nargs=1, type=str,
                        help='esp-appl-id')
    parser.add_argument('--message', nargs=1, type=str,
                        help='Provide description for bmrs')

    parser.add_argument('--export', action='store_true',
                        help='Export hadoop table to teradata. '
                             'required: --db {db}, '
                             'name of db you want to export, '
                             '--table {table}, name of table '
                             'you want to export, --to {db}.{table}, '
                             'name of database and '
                             'table to export to')
    parser.add_argument('--to', nargs=1, type=str,
                        help='Used to provide {database}.{table} '
                             'to export to in Teradata')

    parser.add_argument('--auth-test', action='store_true',
                        help='Test sqoop auth'
                             'required: --source-db {db}, name of db'
                             ' you want to export,'
                             '--source-table {table}, name of table '
                             'you want to export,'
                             '--jdbc-url {jdbcurl}, connection string '
                             'for target schema'
                             '--user-name {user_name}, db user name'
                             '--password-file {hdfs_path}, hdfs'
                             ' password file path')

    # Export to Oracle
    parser.add_argument('--export-oracle', action='store_true',
                        help='Export hadoop table to Oracle. '
                             'required: --source-db {db}, name of db '
                             'you want to export,'
                             '--source-table {table}, name of table '
                             'you want to export,'
                             '--source-dir {dir}, hdfs location of '
                             'export table,'
                             '--jdbc-url {jdbcurl}, connection string'
                             ' for target schema,'
                             '--target-schema {targetdb}, oracle schema,'
                             '--target-table {targettable}, oracle table'
                             '--update-key {updatekey}, non mandatory'
                             ' param - primary key on target table,'
                             '--user-name {username}, oracle username,'
                             '--pass-alias {passalias}, oracle password'
                             ' alias or jceks url')
    parser.add_argument('--source-db', nargs=1, type=str,
                        help='Used to provide source hive schema to'
                             ' export to oracle')
    parser.add_argument('--source-table', nargs=1, type=str,
                        help='Used to provide source hive table to '
                             'export to oracle')
    parser.add_argument('--source-dir', nargs=1, type=str,
                        help='Used to provide hdfs source directory'
                             ' to export to'
                             'oracle, directory should not include'
                             ' the final table directory')
    parser.add_argument('--jdbc-url', nargs=1, type=str,
                        help='Used to provide oracle connection '
                             'information to export to oracle')
    parser.add_argument('--target-schema', nargs=1, type=str,
                        help='Used to provide oracle target schema '
                             'to export to oracle')
    parser.add_argument('--target-table', nargs=1, type=str,
                        help='Used to provide oracle target table to '
                             'export to oracle')
    parser.add_argument('--update-key', nargs='*', type=str,
                        help='Used to provide oracle primary key to'
                             ' export to oracle')
    parser.add_argument('--user-name', nargs=1, type=str,
                        help='Used to provide oracle user name to export'
                             ' to oracle')
    parser.add_argument('--password-file', nargs=1, type=str,
                        help='Used to provide oracle password file')
    parser.add_argument('--pass-alias', nargs=1, type=str,
                        help='Used to provide oracle password alias to'
                             ' export to oracle')
    parser.add_argument('--source-type', nargs=1, type=str,
                        help='Used to provide source vendor type')

    # Export to Teradata
    parser.add_argument(
        '--export_teradata', action='store_true',
        help='Export hadoop table to Teradata. '
             'required: --source-db {db}, name of db you want to export,'
             '--source-table {table}, name of table you want to export,'
             '--source-dir {dir}, hdfs location of export table,'
             '--jdbc-url {jdbcurl}, connection string for target schema,'
             '--target-schema {targetdb}, teradata Database,'
             '--target-table {targettable}, teradata table'
             '--user-name {username}, oracle username,'
             '--pass-alias {passalias}, oracle password alias or jceks url')

    # Generate IT request file input file
    parser.add_argument('--gen-it-table', type=FileType('r'),
                        help='Generate IT table with automatic split-by '
                             'if possible')
    parser.add_argument('--gen-qa-data-sampling', type=FileType('r'),
                        help='Generate workflow for QA data sampling')
    parser.add_argument('--parse-request-file', type=FileType('r'),
                        help='Print each table in request file as json')

    # Workflow actions
    parser.add_argument('--hive', nargs='*', type=str,
                        help='Generates hive action workflow')
    parser.add_argument('--shell', nargs='*', type=str,
                        help='Generates shell action workflow')
    parser.add_argument('--impala', nargs='*', type=str,
                        help='Generate impala action workflow')
    parser.add_argument('--gen-action', nargs='*', type=str,
                        help='Generates action for hive,shell,impala '
                             'in one xml')

    # Copy backup files to live
    parser.add_argument('--retrieve-backup', action='store_true',
                        help='Copies backup files to live. required: '
                             '--db {name} --table {name}')

    # Update freq_ingest Activator
    parser.add_argument('--update-activator', action='store_true',
                        help='provide team frequency, Activator(yes/no), '
                        'team name and full table name')

    # Drop all the table from selected database
    parser.add_argument('--wipe-perf-env', nargs=1, type=str,
                        help='Provide the team_name or database '
                        'name for dropping all tables')

    parser.add_argument('--reingest-all', action='store_true',
                        help='Use this option with wipe-perf-env to '
                        'reingest all tables')

    # Not saving workflows to git
    parser.add_argument('--no-git', action='store_true',
                        help='Not saving workflow to git')
    # No dry run workflow
    parser.add_argument('--no-dry-run', action='store_true',
                        help='Dont dry run workflow')

    parser.add_argument('--timeout', type=str,
                        help='Timeout duration for auto split by')
    parser.add_argument('--ingest-version', action='store_true',
                        help='Get the ingest version used for the xml')
    parser.add_argument('--kite-ingest', type=FileType('r'),
                        help='Used to generate kite-ingest workflow')

    args = parser.parse_args()

    usr_opts = vars(args)
    # Filter usr_opt of None values
    usr_opts = {k: usr_opts[k] for k in usr_opts if usr_opts[k] is not None}
    # Filter usr_opt of False values
    usr_opts = {k: usr_opts[k] for k in usr_opts if usr_opts[k] is not False}

    ibis_opts = {
        'checks_balances': checks_balances,
        'export': export,
        'gen_esp_workflow_tables': gen_esp_workflow_tables,
        'update_activator': update_activator,
        'wipe_perf_env': wipe_perf_env,
        'gen_esp_workflow': gen_esp_workflow,
        'gen_config_workflow': gen_config_workflow,
        'retrieve_backup': retrieve_backup,
        'run_job': run_job,
        'gen_it_table': gen_it_table,
        'submit_request': submit_request,
        'export_request': export_request,
        'export_oracle': export_oracle,
        'save_it_table': save_it_table,
        'update_it_table': update_it_table,
        'update_it_table_export': update_it_table_export,
        'auth_test': auth_test,
        'ingest_version': ingest_version,
        'parse_request_file': parse_request_file,
        'kite_ingest': gen_kite_workflow
    }

    is_failed = False
    if args.env:
        cfg_mgr = ConfigManager(args.env[0], args.for_env)
        file_permission = 0774

        if not os.path.isdir(cfg_mgr.files):
            os.mkdir(cfg_mgr.files)
            os.chmod(cfg_mgr.files, file_permission)
        if not os.path.isdir(cfg_mgr.logs):
            os.mkdir(cfg_mgr.logs)
            os.chmod(cfg_mgr.logs, file_permission)
        if not os.path.isdir(cfg_mgr.saves):
            os.mkdir(cfg_mgr.saves)
            os.chmod(cfg_mgr.saves, file_permission)

        # clear log file
        with open(cfg_mgr.log_file, 'wb'):
            pass
        logger = get_logger(cfg_mgr)

        driver = Driver(cfg_mgr)

        try:
            # Utilize ibis_opts to call correct function(s)
            for key in usr_opts.keys():
                if ibis_opts.get(key, None):
                    # call the appropriate method
                    success = ibis_opts[key](args)
                    if success is False:
                        is_failed = True
            inventory.Inventory.close()
        except Exception:
            logger.error('\n' + traceback.format_exc())
            is_failed = True

        # print the log
        with open(cfg_mgr.log_file, 'rb') as file_handler:
            log_text = file_handler.read()
            if log_text:
                print '+' * 20
                print 'Printing ibis.log'
                print '=' * 20
                print log_text
                print '+' * 20
    else:
        is_failed = True
        err_msg = ('Environment required for ibis. '
                   'Please specify --env argument and provide a environment.')
        print err_msg

    if is_failed:
        # expose ibis failure to the calling env
        sys.exit(1)


def gen_config_workflow(args):
    """Generate config based workflows"""
    config_workflow_properties = None
    queue_name = None
    if args.config_workflow_properties:
        config_workflow_properties = args.config_workflow_properties[0]
    if args.queue_name:
        queue_name = args.queue_name[0]
    driver.gen_config_workflow(args.gen_config_workflow[0],
                               config_workflow_properties, queue_name)


def auth_test(args):
    """Tests sqoop auth credentials"""
    driver.auth_test(args.jdbc_url[0], args.source_db[0], args.source_table[0],
                     args.user_name[0], args.password_file[0])


def parse_request_file(args):
    """Parse request file and return tables as json. Useful for web app"""
    driver.parse_request_file(args.parse_request_file)


def checks_balances(args):
    """Handler for Checks and balances."""
    if args.db and args.table and args.update_lifespan:
        print driver.update_lifespan(args.db[0], args.table[0],
                                     ','.join(args.update_lifespan))
    elif args.update_all_lifespan:
        print driver.update_all_lifespan()
    else:
        print 'Please provide a --db and --table and option[--update-lifespan]'


def export(args):
    """Handler for export."""
    if args.db and args.table and args.to:
        print driver.export(args.db[0], args.table[0], args.to[0])
    else:
        print '--export requires --db {db}, --table {table}, and ' \
              '--to {db}.{table}'


def gen_esp_workflow_tables(args):
    """Generate prod workflows for tables in the request file"""
    driver.gen_prod_workflow_tables(args.gen_esp_workflow_tables)


def update_activator(args):
    """Updates the Activator of the of the freq_ingest table"""
    driver.insert_freq_ingest_driver(args.teamname,
                                     args.frequency,
                                     args.table,
                                     args.activate)


def wipe_perf_env(args):
    """Drop all the tables of the selected database and reingest
     all the tables"""
    driver.wipe_perf_env_driver(args.wipe_perf_env,
                               args.reingest_all)


def gen_esp_workflow(args):
    """Handler for esp workflows."""
    if args.gen_esp_workflow:
        for esp_id in args.gen_esp_workflow:
            success, msg, git_files = driver.gen_prod_workflow(esp_id)
            print msg
    else:
        print "--gen-esp-workflow requires at least one ESP id."


def retrieve_backup(args):
    """Handler for backup."""
    if args.db and args.table:
        print driver.retrieve_backup(args.db[0], args.table[0])
    else:
        print '--retrieve-backup requires --db {name} --table {name}'


def run_job(args):
    """Handler for running oozie job."""
    status = driver.run_oozie_job(args.run_job)
    if not status:
        logger.error("Error: Job run failed")


def gen_it_table(args):
    """Generate IT table with split by."""
    if args.gen_it_table:
        print "Generating split by and IT table"
        driver.gen_it_table_with_split_by(tables_fh=args.gen_it_table,
                                          timeout=int(args.timeout))
    else:
        err_msg = "Please provide list of tables file with --split-by"
        print err_msg


def submit_request(args):
    """Generate workflow."""
    status, msg = driver.submit_request(args.submit_request, args.no_git)
    if not status:
        print msg
    return status


def export_oracle(args):
    """Handler for oracle export."""
    if len(args.update_key) == 0:
        args.update_key.append("")
    print driver.export_oracle(args.source_table[0], args.source_db[0],
                               args.source_dir[0], args.jdbc_url[0],
                               args.update_key[0], args.target_table[0],
                               args.target_schema[0], args.user_name[0],
                               args.pass_alias[0])


def save_it_table(args):
    """Handler for saving IT table."""
    print driver.save_it_table(args.source_type[0])


def update_it_table(args):
    """Handler for updating IT table."""
    print driver.submit_it_file(args.update_it_table)


def update_it_table_export(args):
    """Handler for updating IT table."""
    print driver.submit_it_file_export(args.update_it_table_export)


def ingest_version(args):
    """used of deploying ibis"""
    cfg_mgr = ConfigManager(args.env[0], args.env[0], 'True')
    print 'Ingest version--', cfg_mgr.hdfs_ingest_version
    with open('ingest_version.txt', 'w') as file_h:
        file_h.write(cfg_mgr.hdfs_ingest_version)
        file_h.close()
        print 'Ingest version:', cfg_mgr.hdfs_ingest_version


def gen_kite_workflow(args):
    """Generate kite workflow."""
    driver.gen_kite_workflow(args.kite_ingest)


def export_request(args):
    """Generate workflow export request. """
    status, msg = driver.export_request(args.export_request, args.no_git)
    if not status:
        print msg
        raise ValueError("Workflow not generated - reason: %s" % msg)
    print msg


if __name__ == "__main__":
    main()
