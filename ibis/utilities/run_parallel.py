"""Utilities for running code in parallel using multiprocessing"""
import math
from multiprocessing import Pool
import numpy as np
from ibis.custom_logging import get_logger
from ibis.utilities.sqoop_helper import SqoopHelper, SQOOP_CACHE
from ibis.utilities.sqoop_helper import SQOOP_CACHE_VIEW
from ibis.utilities.utilities import Utilities


def parallel_dryrun_workflows(info):
    """Dry run workflows in parallel.
    For sake of multiprocessing.Pool, this needs to be a top level function
    Args:
        info: List[cfg_mgr, workflow.xml]
    """
    cfg_mgr = info[0]
    workflow_name = info[1]
    utils = Utilities(cfg_mgr)
    status = utils.dryrun_workflow(workflow_name)
    return status, workflow_name


def get_split_num(size, max_pool_size):
    """Returns ideal number of splits"""
    splits = 1
    if size > max_pool_size:
        splits = math.ceil(float(size)/float(max_pool_size))
    return splits


class DryRunWorkflowManager(object):
    """Dry run workflows in parallel"""

    def __init__(self, cfg_mgr):
        """init"""
        self.cfg_mgr = cfg_mgr
        self.logger = get_logger(self.cfg_mgr)

    def run_all(self, workflows):
        """Dry run in parallel. Filter files and dry run xmls
        Args:
            workflows: generated files
        """
        status = True
        pool_info = []
        for file_name in workflows:
            if '.xml' in file_name and 'props_job.xml' not in file_name:
                pool_info.append(
                    [self.cfg_mgr, file_name.replace('.xml', '')])

        num_splits = get_split_num(
            len(pool_info), self.cfg_mgr.parallel_dryrun_procs)
        chunks = np.array_split(pool_info, num_splits)

        for pool_chunk in chunks:
            pool_obj = Pool(processes=len(pool_chunk))
            result_list = pool_obj.map(parallel_dryrun_workflows, pool_chunk)
            pool_obj.terminate()
            for info in result_list:
                if not info[0]:
                    err_msg = 'Dry run failed: {0}'.format(info[1])
                    self.logger.error(err_msg)
                    status = status and False
                else:
                    status = status and True
        return status


def parallel_sqoop_output(info):
    """Run sqoop queries in parallel
    For sake of multiprocessing.Pool, this needs to be a top level function
    Args:
        info: List[cfg_mgr, jdbcurl, sql_query, db_username,
                   password_file]
    """
    cfg_mgr = info[0]
    jdbc = info[1]
    sql_stmt = info[2]
    db_username = info[3]
    password_file = info[4]
    sqoop = SqoopHelper(cfg_mgr)
    result = sqoop.eval(jdbc, sql_stmt, db_username, password_file)
    return sql_stmt, result


class SqoopCacheManager(object):
    """Cache sqoop query results"""

    def __init__(self, cfg_mgr):
        """init"""
        self.cfg_mgr = cfg_mgr
        self.logger = get_logger(self.cfg_mgr)

    def cache_ddl_queries(self, tables):
        """Caches DDL queries"""
        global SQOOP_CACHE

        pool_info = []
        for tbl in tables:
            sqoop = SqoopHelper(self.cfg_mgr)
            query = sqoop.get_ddl_query(tbl.jdbcurl, tbl.database,
                                        tbl.table_name, tbl.schema)
            pool_info.append([self.cfg_mgr, tbl.jdbcurl, query,
                              tbl.username, tbl.password_file])

        num_splits = get_split_num(
            len(pool_info), self.cfg_mgr.parallel_sqoop_procs)
        chunks = np.array_split(pool_info, num_splits)

        for pool_chunk in chunks:
            pool_obj = Pool(processes=len(pool_chunk))
            result_list = pool_obj.map(parallel_sqoop_output, pool_chunk)
            pool_obj.terminate()
            for info in result_list:
                SQOOP_CACHE[info[0]] = info[1]

    def cache_ddl_views(self, tables):
        """Caches DDL queries"""
        global SQOOP_CACHE_VIEW

        pool_info = []
        for tbl in tables:
            sqoop = SqoopHelper(self.cfg_mgr)
            if tbl.is_oracle:
                query = sqoop.get_ddl_table_view(tbl.jdbcurl, tbl.database,
                                                 tbl.table_name)
                pool_info.append([self.cfg_mgr, tbl.jdbcurl, query,
                                  tbl.username, tbl.password_file])

        if len(pool_info) > 0:
            num_splits = get_split_num(
                len(pool_info), self.cfg_mgr.parallel_sqoop_procs)
            chunks = np.array_split(pool_info, num_splits)

            for pool_chunk in chunks:
                pool_obj = Pool(processes=len(pool_chunk))
                result_list = pool_obj.map(parallel_sqoop_output, pool_chunk)
                pool_obj.terminate()
                for info in result_list:
                    SQOOP_CACHE_VIEW[info[0]] = info[1]
