#!/usr/bin/env bash
# hadoop fs get fails if the file exists in current directory, so create a fresh dir everytime
RAND_DIR=$(cat /dev/urandom | head -c 2000 | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
RAND_DIR=incr_"$RAND_DIR"

mkdir ${RAND_DIR}
cd ${RAND_DIR}

# WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING
# WRITE CODE BELOW THIS LINE
if [ -z ${hdfs_ingest_path+x} ]; then
    # if the workflows doesnt have hdfs_ingest_path env variable.
    hdfs_ingest_path="/user/dev/oozie/workspaces/ibis/lib/ingest/"
fi

ingest_dir="/user/data/ingest/"

fetch_hdfs_files() {
    hadoop fs -get "$hdfs_ingest_path"config_env.sh
    hadoop fs -get "$hdfs_ingest_path"shell_utils.sh
    hadoop fs -get "$hdfs_ingest_path"sqoop_utils.py
    hadoop fs -get "$hdfs_ingest_path"sql_queries.py
    hadoop fs -get "$hdfs_ingest_path"impala_utils.py
    hadoop fs -get "$hdfs_ingest_path"parquet_opt_ddl_time.py
    hadoop fs -get "$hdfs_ingest_path"zookeeper_remove_locks.py
    hadoop fs -get /user/dev/scratch/fake.keytab
    hadoop fs -get /user/dev/oozie/workspaces/ibis/lib/avro-tools-1.7.5.jar
    hadoop fs -get /user/dev/oozie/workspaces/ibis/lib/velocity-1.7.jar
    hadoop fs -get /user/hive/sentry/sentry-provider.ini .

}

setup_env_vars() {
    # Set configuration variables
    source ./config_env.sh
    source ./shell_utils.sh
    setup_it_table_env
}

create_avro_schema() {
    # Create the Avro table from the schema that was brought in
    hadoop fs -mkdir -p ${ingest_dir}${target_dir}/_gen/

    # Used for avo schema
    export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:velocity-1.7.jar

    # Extract schema from avro data file
    avsc=`hadoop jar ./avro-tools-1.7.5.jar getschema ${ingest_dir}${target_dir}/part-m-00000.avro`

    if [[ ${avsc} == {* ]]
        then
            echo
        else
            avsc=`echo -n ${avsc} | sed -e 's/^[^{]+//'`
    fi

    # Writes the file to strerr for checking
    echo ${avsc} 1>&2
    # Create avsc file
    echo -n ${avsc} > ${source_table_name}.avsc

    # Based on some new configs (CDH 5.5) error messages sometimes get appended
    # to the stop of the Avro schema, so this strips out all those error messages
    echo -n ${avsc} | perl -pe 's|(.*?)(\{.*)|\2|' 2>/dev/null > ${source_table_name}.avsc

    # Move avsc to hdfs in ingest location since this is for the avro files
    hadoop fs -put -f ${source_table_name}.avsc ${ingest_dir}${target_dir}/_gen/

    # Compile java classes from schema
    hadoop jar ./avro-tools-1.7.5.jar compile schema ${source_table_name}.avsc compiled/parquet_stage.hql

    jar -cf ${source_table_name}.jar -C compiled/ .

    # Add jar to _gen folder
    hadoop fs -put -f ${source_table_name}.jar ${ingest_dir}${target_dir}/_gen/

    # Pre-cleanup
    HIVE_SCHEMA="ingest"
    IMPALA_UTIL_METHOD="create"

    CREATE_DB_STRING="CREATE DATABASE IF NOT EXISTS ${HIVE_SCHEMA}"

    python impala_utils.py "${source_database_name}_${source_table_name}" "${CREATE_DB_STRING}" "${IMPALA_UTIL_METHOD}" "impala"

    DROP_OLD_TBL_STRING="DROP TABLE IF EXISTS ${HIVE_SCHEMA}.${source_database_name}_${source_table_name}"

    python impala_utils.py "${source_database_name}_${source_table_name}" "${DROP_OLD_TBL_STRING}" "${IMPALA_UTIL_METHOD}" "impala"

    # Create the Avro table
    QUERY_STRING="CREATE EXTERNAL TABLE ${HIVE_SCHEMA}.${source_database_name}_${source_table_name} STORED AS AVRO LOCATION 'hdfs://${ingest_dir}${target_dir}' TBLPROPERTIES ('avro.schema.url'='hdfs://${ingest_dir}${target_dir}/_gen/${source_table_name}.avsc')"

    python impala_utils.py "${source_database_name}_${source_table_name}" "${QUERY_STRING}" "${IMPALA_UTIL_METHOD}" "impala"

    let EXIT_STATUS_PY=$?

    if [ "${EXIT_STATUS_PY}" -gt 0 ]
    then
        echo -e "FAILED ----------> impala_utils.py\n"
        exit 1
    fi
}

create_hql_file() {
    hadoop fs -get /user/data/${target_dir}/gen/ingest_unix_timestamp.txt
    ingest_time_seconds=`cat ingest_unix_timestamp.txt`
    ingest_time=$(date +"%Y-%m-%d %T" -d @${ingest_time_seconds})
    echo "parquet_opt_ddl_time.py ${target_dir} SQOOPJARS ${hive2_jdbc_url} ${ingest_time} incremental ${QUEUE_NAME}"
    python parquet_opt_ddl_time.py "${target_dir}" "${SQOOPJARS}" "${hive2_jdbc_url}" "${ingest_time}" "incremental" "${QUEUE_NAME}"
    let EXIT_STATUS_PY=$?

    if [ "${EXIT_STATUS_PY}" -gt 0 ]
    then
        echo -e "FAILED ----------> parquet_opt_ddl_time.py\n"
    else
        hadoop fs -put -f incr_create_table.hql /user/data/${target_dir}/gen/

        hadoop fs -put -f incr_merge_partition.hql /user/data/${target_dir}/gen/

        upload_view_files
    fi
}


main() {
    fetch_hdfs_files
    setup_env_vars
    setup_kinit
    setup_sqoop_env
    setup_venv
    create_avro_schema
    create_hql_file
    remove_locks
    deactivate
}

main


# WRITE CODE ABOVE THIS LINE
# WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING

cd ..
rm -rf ${RAND_DIR}

if [ "$EXIT_STATUS_PY" -gt 0 ]
then
    exit 1
fi
