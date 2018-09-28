#!/bin/bash

# hadoop fs get fails if the file exists in current directory, so create a fresh dir everytime
RAND_DIR=$(cat /dev/urandom | head -c 2000 | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
RAND_DIR=export_"$RAND_DIR"

hostname -v

mkdir $RAND_DIR
cd $RAND_DIR


# WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING
# WRITE CODE BELOW THIS LINE


if [ -z ${hdfs_export_path+x} ]; then
    # if the workflows doesnt have hdfs_export_path env variable.
    hdfs_export_path ="/user/dev/oozie/workspaces/ibis/lib/ingest/"
fi


# Set configuration variable
hadoop fs -get "$hdfs_export_path"config_env.sh
source ./config_env.sh

hadoop fs -get /user/dev/scratch/fake.keytab
kinit -S krbtgt/${KERBEROS}.COM@${KERBEROS}.COM fake_username@${KERBEROS}.COM -k -t fake.keytab
export HADOOP_CREDSTORE_PASSWORD="none"

# Clone table statement in hive.
echo -e "SET mapred.job.queue.name=ingestion;\ndrop table if exists ${database}.${table_name}_clone;\ncreate table ${database}.${table_name}_clone row format delimited fields terminated by '\\\\001' escaped by '\\\\\\' stored as textfile location '${EXPORT_HDFS_ROOT}${database}/${table_name}_clone' tblproperties ('serialization.null.format' = '\\\\\N') as select * from ${database}.${table_name};\nanalyze table ${database}.${table_name}_clone compute statistics;" > parquet_export.hql

cat parquet_export.hql

echo -e "set hive.warehouse.subdir.inherit.perms=true;\ndrop table if exists ${database}.${table_name}_clone;" > clone_delete.hql

hiveQuery=`beeline /etc/hive/beeline.properties -u ${hive2_jdbc_url}\;principal=hive/${KERBEROS_PRINCIPAL} --hiveconf mapred.job.queue.name=$queueName --silent=true --showHeader=false --outputformat=csv2 -e "SHOW COLUMNS IN ${database}.${table_name};"`
columnNames=$hiveQuery
columnNames=`echo $columnNames`
columnNames=${columnNames// /,}
echo "columnNames=$columnNames"


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

# Create clone directory to create the text version of the parquet source table
hadoop fs -mkdir ${EXPORT_HDFS_ROOT}${database}/
hadoop fs -mkdir ${EXPORT_HDFS_ROOT}${database}/${table_name}_clone
hadoop fs -mkdir ${EXPORT_HDFS_ROOT}${database}/${table_name}_clone_hql

# remove hql if its still there from the last job
hadoop fs -rm ${EXPORT_HDFS_ROOT}${database}/${table_name}_clone_hql/parquet_export.hql

# move parquet_export.hql to clone location spot in / /gen
hadoop fs -put -f parquet_export.hql ${EXPORT_HDFS_ROOT}${database}/${table_name}_clone_hql/

# remove hql if its still there from the last job
hadoop fs -rm ${EXPORT_HDFS_ROOT}${database}/${table_name}_clone_hql/clone_delete.hql

# move clone_delete.hql to clone location spot in / /gen
hadoop fs -put -f clone_delete.hql ${EXPORT_HDFS_ROOT}${database}/${table_name}_clone_hql/

# WRITE CODE ABOVE THIS LINE
# WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING

cd ..
rm -rf $RAND_DIR
