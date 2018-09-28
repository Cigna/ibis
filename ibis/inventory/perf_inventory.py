"""Perf itTable hql creation and freq_ingest table update"""
import os
import subprocess
import getpass

from ibis.custom_logging import get_logger
from ibis.inventory.inventory import Inventory
from ibis.model.freq_ingest import Freq_Ingest
from ibis.model.table import ItTable
from ibis.utilities.utilities import Utilities


class PerfInventory(Inventory):

    def __init__(self, cfg_mgr):
        self.cfg_mgr = cfg_mgr
        self.utilities = Utilities(self.cfg_mgr)
        self.logger = get_logger(self.cfg_mgr)
        self.queue = self.cfg_mgr.queue_name
        self.table = self.cfg_mgr.it_table
        self.freq_ingest_table = self.cfg_mgr.freq_ingest

    def save_perf_hql(self, table):
        """creates perf view hql"""
        all_file = []
        all_views = table.views_list
        domain = self.cfg_mgr.domains_list
        domain = domain.split(",")
        domain = set(domain).intersection(all_views)
        domain = list(domain)

        for target_db in all_views:
            if len(domain) > 0 and target_db.lower() == domain[0]:
                continue
            view_full_name = '{view_name}.{database}_{table_name}'
            view_full_name = view_full_name.format(
                view_name=target_db, database=table.database,
                table_name=table.table_name)
            src_view = '{src_vw_name}.{src_db}_{src_tbl_name}'
            src_view = src_view.format(
                src_vw_name=table.domain,
                src_db=table.database,
                src_tbl_name=table.table_name)

            if len(domain) > 0 and domain[0]:
                team_view_name = '{team_nm}.{team_db}_{team_table}'
                team_view_name = team_view_name.format(
                    team_nm=domain[0],
                    team_db=table.database,
                    team_table=table.table_name)
                views_hql = (
                    'SET mapred.job.queue.name={queue_name};\n'
                    'SET mapreduce.map.memory.mb=8000;\n'
                    'SET mapreduce.reduce.memory.mb=16000;\n\n'
                    'DROP VIEW IF EXISTS {view_full_name};\n'
                    'DROP TABLE IF EXISTS {view_full_name};\n\n'
                    'CREATE DATABASE IF NOT EXISTS {view_name};\n'
                    'CREATE DATABASE IF NOT EXISTS {team_name};\n'
                    'DROP VIEW IF EXISTS {team_view_name};\n'
                    'DROP TABLE IF EXISTS {team_view_name};\n'
                    'CREATE VIEW {team_view_name} AS '
                    'SELECT * FROM {src_view}; \n '
                    'CREATE  TABLE {view_full_name} like {src_view}\n'
                    'INSERT OVERWRITE TABLE {view_full_name} '
                    'PARTITION(incr_ingest_timestamp) '
                    'select * from {team_view_name} ;\n\n'
                )
                views_hql = views_hql.format(view_full_name=view_full_name,
                                             view_name=target_db,
                                             src_view=src_view,
                                             team_view_name=team_view_name,
                                             team_name=domain[0],
                                             queue_name=self.queue)
            else:
                views_hql = (
                    'SET mapred.job.queue.name={queue_name};\n'
                    'SET mapreduce.map.memory.mb=8000;\n'
                    'SET mapreduce.reduce.memory.mb=16000;\n\n'
                    'DROP VIEW IF EXISTS {view_full_name};\n'
                    'DROP TABLE IF EXISTS {view_full_name};\n\n'
                    'CREATE DATABASE IF NOT EXISTS {view_name};\n'
                    'CREATE  TABLE {view_full_name} like {src_view}\n'
                    'INSERT OVERWRITE TABLE {view_full_name} '
                    'PARTITION(incr_ingest_timestamp) '
                    'select * from {src_view} ;\n\n')
                views_hql = views_hql.format(view_full_name=view_full_name,
                                             view_name=target_db,
                                             src_view=src_view,
                                             queue_name=self.queue)

            views_hql += "msck repair table {0};\n\n".format(
                view_full_name)

            file_name = table.db_env + '_' + getpass.getuser() + '_' + \
                'full_' + target_db + '_' +\
                Utilities.replace_special_chars(table.table_name)\
                + '.hql'
            all_file.append(file_name)
            act_file_name = os.path.join(self.cfg_mgr.files, file_name)
            file_h = open(act_file_name, "wb+")
            file_h.write(views_hql)
            file_h.close()

        return all_file

    def insert_freq_ingest(self, team_name, frequency, table, activate):
        """ Update freq_ingest table
        """

        freq_ingest = Freq_Ingest(team_name, frequency, table, activate)

        if freq_ingest.activator == 'default':
            insert_qry = ("insert overwrite table {table} "
                          "partition(view_name, full_table_name) "
                          "values ('{freq}', 'yes', '{views}', "
                          "'{full_table}')")
            insert_qry = insert_qry.format(views=freq_ingest.view_nm,
                                           freq=freq_ingest.frequency,
                                           full_table=freq_ingest.full_tb_nm,
                                           table=self.freq_ingest_table)
            self.run_query(insert_qry, self.freq_ingest_table)
        else:

            get_freq = ("select * from {table} "
                        "where view_name='{views}' "
                        "and full_table_name='{full_table}'")
            get_freq = get_freq.format(views=freq_ingest.view_nm,
                                       full_table=freq_ingest.full_tb_nm,
                                       table=self.freq_ingest_table)
            self.logger.info(get_freq)
            get_freq = self.get_rows(get_freq)

            if freq_ingest.frequency is None:
                freq_ingest.frequency = get_freq[0][0]

            if freq_ingest.activator is None:
                freq_ingest.activator = get_freq[0][1]

            insert_qry = ("insert overwrite table {table} "
                          "partition(view_name, full_table_name) "
                          "values ('{freq}', '{active}', '{views}', "
                          "'{full_table}')")
            insert_qry = insert_qry.format(views=freq_ingest.view_nm,
                                           freq=freq_ingest.frequency,
                                           full_table=freq_ingest.full_tb_nm,
                                           active=freq_ingest.activator,
                                           table=self.freq_ingest_table)
            self.run_query(insert_qry, self.freq_ingest_table)

    def wipe_perf_env(self, db_name, reingest):
        """
        method to wipe hdfs_database specified in db_name
        reingest specifies if team space needs to be populated
        """
        if reingest:
            del_all_tables = "drop database {0} cascade".format(db_name)
            self.run_query(del_all_tables)

            create_db = " create database {0} ".format(db_name)
            self.run_query(create_db)

            get_all_table = ("select views, source_table_name, "
                             "source_database_name, full_table_name from "
                             "{table} where views like '%{db}%';")
            get_all_table = get_all_table.format(table=self.table,
                                                 db=db_name)
            get_all_table = self.get_rows(get_all_table)
            for get_row in get_all_table:
                get_row_dict = {'views': get_row[0],
                                'source_table_name': get_row[1],
                                'source_database_name': get_row[2],
                                'full_table_name': get_row[3]}
                table = ItTable(get_row_dict, self.cfg_mgr)
                all_views = table.views_list
                for views in all_views:
                    if views == db_name:
                        file_name = self.reingest_hql(db_name,
                                                      get_row[1],
                                                      get_row[2],
                                                      get_row[3])
                        self.run_ingest_hql(file_name)

        else:
            del_all_tables = "drop database {0} cascade".format(db_name)
            self.run_query(del_all_tables)
            create_db = " create database {0} ".format(db_name)
            self.run_query(create_db)

    def reingest_hql(self, view, src_table, src_db, full_table):
        """Create HQLs for all table to reingest with row data from it_table
        @view : views from ibis it_table provides team name
        @src_table : source_table_name from ibis it_table for table name
        @src_db : source_database_name from ibis it_table for db name
        @full_table : full_table_name from ibis it_table for hdfs table name
        """
        view_full_name = '{view_name}.{database}_{table_name}'
        view_full_name = view_full_name.format(
            view_name=view, database=src_db,
            table_name=src_table)
        src_view = full_table
        views_hql = (
            'SET mapred.job.queue.name={queue_name};\n'
            'SET mapreduce.map.memory.mb=8000;\n'
            'SET mapreduce.reduce.memory.mb=16000;\n\n'
            'DROP VIEW IF EXISTS {view_full_name};\n\n'
            'DROP TABLE IF EXISTS {view_full_name};\n\n'
            'set hive.exec.dynamic.partition.mode=nonstrict;\n\n'
            'CREATE DATABASE IF NOT EXISTS {view_name};\n\n'
            'CREATE  TABLE {view_full_name} like {src_view};\n\n'
            'INSERT OVERWRITE TABLE {view_full_name} '
            'PARTITION(incr_ingest_timestamp) '
            'select * from {src_view} ;\n\n'
        )
        views_hql = views_hql.format(queue_name=self.queue,
                                     view_full_name=view_full_name,
                                     view_name=view,
                                     src_view=src_view)

        file_name = view + '_' + getpass.getuser() + '_' + \
            'full_' + Utilities.replace_special_chars(src_table)+'.hql'
        act_file_name = os.path.join(self.cfg_mgr.files, file_name)
        file_h = open(act_file_name, "wb+")
        file_h.write(views_hql)
        file_h.close()
        return act_file_name

    def run_ingest_hql(self, team_hql):
        """ Run all the HQLs generated for reingest """

        jdbc_url_param = "-u '{0}'".format(self.cfg_mgr.beeline_url)
        file_name = " -f '{0}'".format(team_hql)
        eval_params = ["beeline", "/etc/hive/beeline.properties",
                       jdbc_url_param,
                       file_name]
        shell_true = True
        eval_params = " ".join(eval_params)
        proc = subprocess.Popen(eval_params, shell=shell_true)
        _, err = proc.communicate()
        if proc.returncode == 0:
            self.logger.info("Tables ingested successfully")
        else:
            self.logger.error("Error in ingesting tables " + str(err))
