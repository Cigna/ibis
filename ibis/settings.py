"""Settings. """
import subprocess
import os

UNIT_TEST_ENV = 'UNIT_TEST'
INTEGRATION_TEST_ENV = 'INT_TEST'


def get_running_host():
    """Determine the host on which tests are running for tests"""
    host = ''
    proc = subprocess.Popen(['hostname'], stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    output, err = proc.communicate()

    if proc.returncode == 0:
        is_dev = False
        if 'ENV' in os.environ and os.environ['ENV'] == 'dev':
            is_dev = True

        is_int = False
        if 'ENV' in os.environ and os.environ['ENV'] == 'int':
            is_int = True

        if output == 'fake.edgenode\n':
            host = 'fake.impala'
        elif output == 'fake.dev.edgenode\n' or is_dev:
            host = 'fake.dev.impala'
        elif output == 'fake.int.edgenode\n' or is_int:
            host = 'fake.int.impala'
        else:
            print 'Error: unknown host'
    else:
        print output
        print err
        print 'Error: hostname command failed'

    if host:
        print 'hostname: ', host
    else:
        raise ValueError('Error: Could not determine hive host to connect')
    return host


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
            _msg = 'hdfs get failed for /user/dev/oozie/share/lib/sqoop/{0}'
            _msg = _msg.format(jar)
            print _msg
            print output, err

    HADOOP_CLASSPATH = ','.join(HADOOP_CLASSPATH)
    SQOOPJARS = ','.join(SQOOPJARS)

    os.environ['HADOOP_CLASSPATH'] = HADOOP_CLASSPATH
    os.environ['SQOOPJARS'] = SQOOPJARS
    return HADOOP_CLASSPATH, SQOOPJARS

if __name__ == '__main__':
    sqoop_standalone_setup()
