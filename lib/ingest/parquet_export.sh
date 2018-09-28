#!/bin/bash

if [ -z ${hdfs_ingest_path+x} ]; then
    # if the workflows doesnt have hdfs_ingest_path env variable.
    hdfs_ingest_path="/user/dev/oozie/workspaces/ibis/lib/ingest/"
fi

#Set configuration variables
hadoop fs -get "$hdfs_ingest_path"config_env.sh
source ./config_env.sh

#Clone table statement in hive.
echo -e "drop table if exists ${database}.${table_name}_clone; create table ${database}.${table_name}_clone row format delimited fields terminated by '|' escaped by '\\\\\\' stored as textfile location '/tmp/${table_name}_clone' as select distinct * from ${database}.${table_name};  analyze table ${database}.${table_name}_clone compute statistics;" > parquet_export.hql

# Get sqoop eval libraries
if [[ ! -f "/tmp/jars/db2jcc.jar" || ! -f "/tmp/jars/ojdbc6.jar" || \
    ! -f "/tmp/jars/sqoop-connector-teradata-1.5c5.jar" || ! -f "/tmp/jars/terajdbc4.jar" || \
    ! -f "/tmp/jars/jtds.jar" || ! -f "/tmp/jars/oraoop-1.6.0.jar" || ! -f "/tmp/jars/tdgssconfig.jar" || \
    ! -f "/tmp/jars/db2jcc4.jar" || ! -f "/tmp/jars/sqljdbc4.jar" || ! -f "/tmp/jars/mysql-connector-java-5.1.31-bin.jar" ]]; then

    if [ ! -d "/tmp/jars" ]; then
      mkdir /tmp/jars/
    fi

    hadoop fs -get /user/dev/oozie/share/lib/sqoop/db2jcc.jar
    mv -f db2jcc.jar /tmp/jars/
    hadoop fs -get /user/dev/oozie/share/lib/sqoop/ojdbc6.jar
    mv -f ojdbc6.jar /tmp/jars/
    hadoop fs -get /user/dev/oozie/share/lib/sqoop/sqoop-connector-teradata-1.5c5.jar
    mv -f sqoop-connector-teradata-1.5c5.jar /tmp/jars/
    hadoop fs -get /user/dev/oozie/share/lib/sqoop/terajdbc4.jar
    mv -f terajdbc4.jar /tmp/jars/
    hadoop fs -get /user/dev/oozie/share/lib/sqoop/jtds.jar
    mv -f jtds.jar /tmp/jars/
    hadoop fs -get /user/dev/oozie/share/lib/sqoop/oraoop-1.6.0.jar
    mv -f oraoop-1.6.0.jar /tmp/jars/
    hadoop fs -get /user/dev/oozie/share/lib/sqoop/tdgssconfig.jar
    mv -f tdgssconfig.jar /tmp/jars/
    hadoop fs -get /user/dev/oozie/share/lib/sqoop/db2jcc4.jar
    mv -f db2jcc4.jar /tmp/jars/
    hadoop fs -get /user/dev/oozie/share/lib/sqoop/sqljdbc4.jar
    mv -f sqljdbc4.jar /tmp/jars/
    hadoop fs -get /user/dev/oozie/share/lib/sqoop/mysql-connector-java-5.1.31-bin.jar
    mv -f mysql-connector-java-5.1.31-bin.jar /tmp/jars/
    chmod -R 770 /tmp/jars/
    chown -R :fake_group /tmp/jars/
fi

echo  "========================================================================================"
echo "Kerberos is $KERBEROS"
echo "Hadoop Classpath $HADOOP_CLASSPATH"
echo "Sqoop jars $SQOOPJARS"
echo "Running parquet export....."
echo  "========================================================================================"

#Create clone directory to create the text version of the parquet source table
hadoop fs -mkdir /tmp/${table_name}_clone
hadoop fs -mkdir /tmp/${table_name}_clone_hql
#remove hql if its still there from the last job
hadoop fs -rm $/tmp/${table_name}_clone_hql/parquet_export.hql
#move parquet_export.hql to clone location spot in / /gen
hadoop fs -put -f parquet_export.hql /tmp/${table_name}_clone_hql/.

