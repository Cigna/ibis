#!/bin/bash
# runs quality_assurance.py script

# hadoop fs get fails if the file exists in current directory, so create a fresh dir everytime
RAND_DIR=$(cat /dev/urandom | head -c 2000 | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
RAND_DIR=qa_"$RAND_DIR"
echo $RAND_DIR

mkdir $RAND_DIR
cd $RAND_DIR

# WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING
# WRITE CODE BELOW THIS LINE

echo "$USER"
hostname -v

if [ -z ${hdfs_ingest_path+x} ]; then
    # if the workflows doesnt have hdfs_ingest_path env variable.
    hdfs_ingest_path="/user/dev/oozie/workspaces/ibis/lib/ingest/"
fi

fetch_hdfs_files() {
    hadoop fs -get "$hdfs_ingest_path"impala_utils.py
    hadoop fs -get "$hdfs_ingest_path"sql_queries.py
    hadoop fs -get "$hdfs_ingest_path"quality_assurance.py
    hadoop fs -get "$hdfs_ingest_path"shell_utils.sh
    hadoop fs -get "$hdfs_ingest_path"config_env.sh
    hadoop fs -get "$hdfs_ingest_path"oozie_ws_helper.py
    hadoop fs -get "$hdfs_ingest_path"sqoop_utils.py
    hadoop fs -get "$hdfs_ingest_path"py_hdfs.py
    hadoop fs -get /user/dev/scratch/fake.keytab
}


setup_env_vars() {
    # Set configuration variables
    source ./config_env.sh
    source ./shell_utils.sh
    echo  "================"
    echo "Kerberos is $KERBEROS"
    echo "Running quality assurance....."
    echo  "================"
    setup_it_table_env
}

do_qa() {
    if [ -z ${ingestion_type+x} ]; then
        # if the workflow is for full ingestion
        ingestion_type="full_ingest"
    fi

    echo -e "python quality_assurance.py ${source_database_name} ${source_table_name} SQOOPJARS ${jdbcurl} ${connection_factories} ${db_username} ${password_file} ${domain} ${ingestion_type} ${source_schema_name} ${oozie_url} ${workflowName} ${QA_RESULTS_DIR}"
    python quality_assurance.py "${source_database_name}" "${source_table_name}" "${SQOOPJARS}" "${jdbcurl}" "${connection_factories}" "${db_username}" "${password_file}" "${domain}" "${ingestion_type}" "${source_schema_name}" "${oozie_url}" "${workflowName}" "${QA_RESULTS_DIR}"
    let EXIT_STATUS_PY_QA=$?

    if [ "$EXIT_STATUS_PY_QA" -gt 0 ]
    then
        echo -e "------> QA Failed.\n"
    fi
    echo -e 'Printing qa.log\n'
    cat qa.log
}

main() {
    fetch_hdfs_files
    setup_env_vars
    setup_kinit
    setup_sqoop_env
    setup_venv
    do_qa
    deactivate
}

main

# WRITE CODE ABOVE THIS LINE
# WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING

cd ..
rm -rf $RAND_DIR

if [ "$EXIT_STATUS_PY_QA" -gt 0 ]
then
    if [ "${ingestion_type}" != "full_ingest_qa_sampling" ]
    then
        exit 1
    fi
fi
