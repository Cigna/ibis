#!/bin/bash

# hadoop fs get fails if the file exists in current directory, so create a fresh dir everytime
RAND_DIR=$(cat /dev/urandom | head -c 2000 | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
RAND_DIR=avro_"$RAND_DIR"

hostname -v

mkdir $RAND_DIR
cd $RAND_DIR

# WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING
# WRITE CODE BELOW THIS LINE

fetch_hdfs_files() {
    hadoop fs -get "$hdfs_ingest_path"config_env.sh
    hadoop fs -get "$hdfs_ingest_path"shell_utils.sh
    hadoop fs -get "$hdfs_ingest_path"sqoop_utils.py
    hadoop fs -get "$hdfs_ingest_path"sql_queries.py
    hadoop fs -get "$hdfs_ingest_path"impala_utils.py
    hadoop fs -get "$hdfs_ingest_path"zookeeper_remove_locks.py
    hadoop fs -get /user/dev/oozie/workspaces/ibis/lib/avro-tools-1.7.5.jar
    hadoop fs -get /user/dev/oozie/workspaces/ibis/lib/velocity-1.7.jar
    hadoop fs -get "$hdfs_ingest_path"parquet_opt_ddl_time.py
    hadoop fs -get /user/dev/scratch/fake.keytab
    hadoop fs -get /user/hive/sentry/sentry-provider.ini .
}


setup_env_vars() {
    if [ -z ${hdfs_ingest_path+x} ]; then
        # if the workflows doesnt have hdfs_ingest_path env variable.
        hdfs_ingest_path="/user/dev/oozie/workspaces/ibis/lib/ingest/"
    fi

    source ./config_env.sh
    source ./shell_utils.sh

    setup_it_table_env

    export HADOOP_OPTS="-Dmapreduce.job.credentials.binary=$HADOOP_TOKEN_FILE_LOCATION"
    # Used for avo schema
    export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:velocity-1.7.jar

    # get ready for the timing of the parquet step
    # Pull the time in the correct format for the timestamp
    ingest_time_seconds=$(date +%s)
    ingest_time=$(date +"%Y-%m-%d %T" -d @${ingest_time_seconds})
}

create_directories() {
    # Delete staging folder if it exits to get ready for the parquet step
    # Skip trash is used since you do not have access to oozie's trash
    hadoop fs -mkdir -p /user/data/${target_dir}/stage
    hadoop fs -rm -r /user/data/${target_dir}/stage
    hadoop fs -mkdir -p /user/data/${target_dir}/stage

    # Create _gen in live and move old gen folder to _gen
    hadoop fs -mkdir -p /user/data/${target_dir}/live/_gen
    hadoop fs -rm -r /user/data/${target_dir}/live/_gen
    hadoop fs -mkdir -p /user/data/${target_dir}/live/_gen
    hadoop fs -mkdir -p /user/data/${target_dir}/gen
    # hadoop fs -mv /user/data/${target_dir}/gen /user/data/${target_dir}/live/
    # Delete old gen to get ready for new gen files
    hadoop fs -mkdir -p /user/data/${target_dir}/gen
    hadoop fs -rm -r /user/data/${target_dir}/gen

    # Create gen directories
    hadoop fs -mkdir -p /user/data/ingest/${target_dir}/_gen/
    hadoop fs -mkdir -p /user/data/${target_dir}/gen/
    # We need to take ownership of ingest to allow for impala to read out of it
    hadoop fs -chown -R fake_username:fake_group /user/data/ingest/${target_dir}
    hadoop fs -chmod -R 770 /user/data/ingest/${target_dir}
}


create_avro_schema() {
    # Extract schema from avro data file
    avsc=`hadoop jar ./avro-tools-1.7.5.jar getschema /user/data/ingest/${target_dir}/part-m-00000.avro`

    if [[ $avsc == {* ]]
    then
        echo
    else
        avsc=`echo -n $avsc | sed -e 's/^[^{]+//'`
    fi
    # Writes the file to strerr for checking
    echo $avsc 1>&2
    # create avsc file
    echo -n $avsc > ${source_table_name}.avsc

    echo -n $avsc | perl -pe 's|(.*?)(\{.*)|\2|' 2>/dev/null > ${source_table_name}.avsc


    # move avsc to hdfs in ingest location since this is for the avro files
    hadoop fs -put -f ${source_table_name}.avsc /user/data/ingest/${target_dir}/_gen/

    # Compile java classes from schema
    hadoop jar ./avro-tools-1.7.5.jar compile schema ${source_table_name}.avsc compiled/parquet_stage.hql

    jar -cf ${source_table_name}.jar -C compiled/ .
    # add jar to _gen folder
    hadoop fs -put -f ${source_table_name}.jar /user/data/ingest/${target_dir}/_gen/
}


remove_view_files() {
    # remove views hql file in case views are removed for the table
    hadoop fs -rm -r /user/data/${target_dir}/gen/views_${source_table_name}.hql
    hadoop fs -rm -r /user/data/${target_dir}/gen/views_${source_table_name}_invalidate.txt
    hadoop fs -rm -r /user/data/${target_dir}/gen/views_${source_table_name}_info.txt
}

create_parquet_stage_table() {
    # run parquet_opt_ddl_time.py passing in variables needed for the generation of the parquet conversion.
    echo -e "python parquet_opt_ddl_time.py ${target_dir} SQOOPJARS ${hive2_jdbc_url} ${ingest_time} full_load ${QUEUE_NAME}"
    python parquet_opt_ddl_time.py "${target_dir}" "${SQOOPJARS}" "${hive2_jdbc_url}" "${ingest_time}" "full_load" "${QUEUE_NAME}"

    let EXIT_STATUS_PY=$?

    if [ "${EXIT_STATUS_PY}" -gt 0 ]
    then
        echo -e "FAILED ----------> parquet_opt_ddl_time.py\n"
    fi

    # move avro_parquet.hql to final spot in /gen
    hadoop fs -put -f avro_parquet.hql /user/data/${target_dir}/gen/
    # move parquet_live.hql to final spot in / /gen
    hadoop fs -put -f parquet_live.hql /user/data/${target_dir}/gen/
}


main() {
    fetch_hdfs_files
    setup_env_vars
    create_directories
    create_avro_schema
    setup_kinit
    # setup python virtual env
    setup_venv
    # get the JARS
    setup_sqoop_env
    remove_view_files
    create_parquet_stage_table
    upload_view_files
    remove_locks
    deactivate
}

main

# WRITE CODE ABOVE THIS LINE
# WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING

cd ..
rm -rf $RAND_DIR


if [ "${EXIT_STATUS_PY}" -gt 0 ]
then
    exit 1
fi

