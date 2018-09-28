#!/usr/bin/env bash
# hadoop fs get fails if the file exists in current directory, so create a fresh dir everytime
OLD_DIR=$PWD
RAND_DIR=$(cat /dev/urandom | head -c 2000 | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
RAND_DIR=incr_"$RAND_DIR"
echo $RAND_DIR

mkdir $RAND_DIR
cd $RAND_DIR

# WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING
# WRITE CODE BELOW THIS LINE
if [ -z ${hdfs_ingest_path+x} ]; then
    # if the workflows doesnt have hdfs_ingest_path env variable.
    hdfs_ingest_path="/user/dev/oozie/workspaces/ibis/lib/ingest/"
fi

fetch_hdfs_files() {
    hadoop fs -get "$hdfs_ingest_path"config_env.sh > /dev/null 2>&1
    hadoop fs -get "$hdfs_ingest_path"shell_utils.sh > /dev/null 2>&1
    hadoop fs -get "$hdfs_ingest_path"sqoop_utils.py > /dev/null 2>&1
    hadoop fs -get "$hdfs_ingest_path"impala_utils.py > /dev/null 2>&1
    hadoop fs -get "$hdfs_ingest_path"import_prep.py > /dev/null 2>&1
    hadoop fs -get "$hdfs_ingest_path"sql_queries.py > /dev/null 2>&1
    hadoop fs -get /user/dev/scratch/fake.keytab > /dev/null 2>&1
}

setup_env_vars() {
    # Set configuration variables
    source ./config_env.sh
    source ./shell_utils.sh
    setup_sqoop_env
    check_column=`${check_column} | awk '{print toupper($0)}'`
    echo ${check_column}
    setup_ingest_timestamp
}

setup_ingest_timestamp() {
    ingest_time_seconds=$(date +%s)
    echo "${ingest_time_seconds}" > ingest_unix_timestamp.txt
    # To maintain consistent ingest_timestamp even if sqoop rolls
    # over past 12:00 AM, we calculate the ingest_timestamp here.
    # ingest_unix_timestamp.txt will be used after sqoop in incr_post_import.py
    hadoop fs -put -f ingest_unix_timestamp.txt /user/data/${target_dir}/gen/
    let EXIT_STATUS=$?

    if [ "${EXIT_STATUS}" -gt 0 ]
    then
        echo -e "FAILED ----------> incr_import_prep.py - setup_ingest_timestamp\n"
        exit 1
    fi
    # format: 201701010901
    partition_name=$(date +"%Y%m%d%H%M%S" -d @${ingest_time_seconds})
    echo "Incr partition name: $partition_name"
}

delete_target_dir() {
    hadoop fs -rm -r "/user/data/ingest/${target_dir}"
}

# CONVERT THIS TO USE IMPLA_UTILS
fetch_last_value() {
    if [[ "$incremental_mode" = "lastmodified" ]]
    then
        # incremental mode - last modified
        # Get most recent timestamp and append 1 millisecond to avoid retrieving duplicate records which matched most recent timestamp
        hiveQuery=`beeline /etc/hive/beeline.properties -u ${hive2_jdbc_url}\;principal=hive/${KERBEROS_PRINCIPAL} --hiveconf mapred.job.queue.name=$queueName --silent=true --showHeader=false --outputformat=csv2 -e "SELECT CAST(CAST(MAX(${check_column}) AS DOUBLE) AS TIMESTAMP) FROM ${clean_full_name};"`
    else
        # incremental mode - append
        # Get max integer value and increment by 1 to avoid retrieving duplicate records

        hiveQuery=`beeline /etc/hive/beeline.properties -u ${hive2_jdbc_url}\;principal=hive/${KERBEROS_PRINCIPAL} --hiveconf mapred.job.queue.name=$queueName --silent=true --showHeader=false --outputformat=csv2 -e "SELECT MAX(${check_column}) FROM ${clean_full_name};"`
    fi
    sqoopLstUpd=$hiveQuery
    echo "sqoopLstUpd=$sqoopLstUpd"
}

# THIS CAN CAUSE AN ISSUE --> what if the date rolls at midnight?
# Delete partition first before getting the get_count()
delete_partition() {
    # To make this workflow rerunnable we would want to delete the partition that is going to be created if it already exists so when we check for the max(check-column) it wont look at today's partition
    hiveQuery=`beeline /etc/hive/beeline.properties -u ${hive2_jdbc_url}\;principal=hive/${KERBEROS_PRINCIPAL} --hiveconf mapred.job.queue.name=$queueName --silent=true --showHeader=false --outputformat=csv2 -e "ALTER TABLE ${clean_full_name} DROP IF EXISTS PARTITION (incr_ingest_timestamp=\"$partition_name\");"`
}

main() {
    fetch_hdfs_files
    setup_env_vars
    setup_venv
    setup_kinit
    cache_table_props
    setup_it_table_env
    delete_target_dir
    change_repull_status_to_inprogress
    #delete_partition
    fetch_last_value
    deactivate
}

main

# WRITE CODE ABOVE THIS LINE
# WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING

cd ..
rm -rf $RAND_DIR


if [[ "$EXIT_STATUS_PY" -gt 0 ]]
then
    exit 1
fi

