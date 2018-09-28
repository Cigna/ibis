"""Test Sqoop auth credentials"""
import os
import subprocess
from ibis.utilities.sqoop_helper import SqoopHelper, ORACLE
from ibis.custom_logging import get_logger


def sqoop_standalone_setup():
    """setsup sqoop jars"""
    jars = [
        'db2jcc4.jar',
        'ojdbc6.jar',
        'sqoop-connector-teradata-1.5c5.jar',
        'terajdbc4.jar',
        'jtds.jar',
        'oraoop-1.6.0.jar',
        'tdgssconfig.jar',
        'sqljdbc4.jar',
        'db2jcc4_license_cisuz-1.0.jar',
        'db2jcc_javax.jar',
        'db2jcc_license_cu.jar',
        'db2policy.jar',
        'db2qgjava.jar'
    ]

    HADOOP_CLASSPATH = []
    SQOOPJARS = []

    pwd = 'pwd'
    new_line_current_dir = subprocess.check_output(pwd)
    current_dir = new_line_current_dir.strip()
    if not os.path.isdir("{0}/sqoop_jars".format(current_dir)):
        make_sqoop_dir = ["mkdir", "{0}/sqoop_jars".format(current_dir)]
        sqoop_dir = subprocess.Popen(make_sqoop_dir, stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)
        output, err = sqoop_dir.communicate()
        print output, err

    for jar in jars:
        HADOOP_CLASSPATH.append('{0}/sqoop_jars/{1}'.format(current_dir, jar))
        SQOOPJARS.append('{0}/sqoop_jars/{1}'.format(current_dir, jar))
        if os.path.isfile('{0}/sqoop_jars/{1}'.format(current_dir, jar)):
            continue
        hdfs_get = ["hadoop", "fs", "-get",
                    "/user/dev/oozie/share/lib/sqoop/{0}".format(jar),
                    "{0}/sqoop_jars".format(current_dir)]
        proc = subprocess.Popen(hdfs_get, stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        output, err = proc.communicate()

        if proc.returncode != 0:
            print 'hdfs get failed for \
                   /user/dev/oozie/share/lib/sqoop/{0}'.format(jar)
            print output, err

    HADOOP_CLASSPATH = ','.join(HADOOP_CLASSPATH)
    SQOOPJARS = ','.join(SQOOPJARS)

    os.environ['HADOOP_CLASSPATH'] = HADOOP_CLASSPATH
    os.environ['SQOOPJARS'] = SQOOPJARS


class AuthTest(object):
    """Tests Sqoop Auth"""

    def __init__(self, cfg_mgr, database, table, jdbcurl):
        """Init"""
        self.cfg_mgr = cfg_mgr
        self.jdbcurl = jdbcurl
        self.database = database
        self.table = table
        self.logger = get_logger(self.cfg_mgr)

    def verify(self, user_name, password_file):
        """Test auth"""
        status = True
        queries = []
        queries.append('SELECT COUNT(*) FROM {0}.{1} WHERE 1=0'.format(
            self.database.upper(), self.table.upper()))

        if ORACLE in self.jdbcurl:
            queries.append('select * from sys.v_$instance where rownum < 10')
            queries.append('select * from dba_tables where rownum < 10')
            queries.append('select * from dba_tab_columns where rownum < 10')
            queries.append('select * from dba_objects where rownum < 10')
            queries.append('select * from dba_extents where rownum < 10')
            queries.append('select * from dba_segments where rownum <10')
            queries.append(
                'select * from dba_tab_subpartitions where rownum < 10')
            queries.append('select * from sys.v_$database where rownum < 10')
            queries.append('select * from sys.v_$parameter where rownum < 10')

        sqoop_h = SqoopHelper(self.cfg_mgr)

        for query in queries:
            ret, _, err = sqoop_h._eval(self.jdbcurl, query, user_name,
                                        password_file)
            self.logger.info('Running query: {0}'.format(query))
            if ret == 0:
                msg = 'Success: {0} has SELECT access'
                msg = msg.format(user_name)
                self.logger.info(msg)
            else:
                if status:
                    status = False
                if 'ORA-01017' in err or 'ORA-00942' in err \
                        or 'Error 8017' in err \
                        or 'ERRORCODE=-4214' in err \
                        or 'Login failed for user' in err:
                    msg = "FAILED: {0} doesn't have SELECT access"
                    msg = msg.format(user_name)
                    self.logger.warning(msg)
                else:
                    self.logger.error(err)
                    self.logger.error('FAILED. Re-check jdbcurl,'
                                      ' database, table')
            self.logger.info('-' * 100)
        return status
