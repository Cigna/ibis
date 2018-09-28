#!/bin/bash 
# runs quality_assurance_export.py script

# hadoop fs get fails if the file exists in current directory, so create a fresh dir everytime
RAND_DIR=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
RAND_DIR=qa_"$RAND_DIR"
echo $RAND_DIR

mkdir $RAND_DIR
cd $RAND_DIR

# WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING
# WRITE CODE BELOW THIS LINE

echo "$USER"
hostname -v

if [ -z ${hdfs_export_path+x} ]; then
    # if the workflows doesnt have hdfs_export_path env variable.
    hdfs_export_path="/user/dev/oozie/workspaces/ibis/lib/ingest/"
fi

fetch_hdfs_files() {
    hadoop fs -get "${hdfs_export_path}"impala_utils.py
    hadoop fs -get "${hdfs_export_path}"sql_queries.py
    hadoop fs -get "${hdfs_export_path}"quality_assurance_export.py
    hadoop fs -get "${hdfs_export_path}"checks_and_balances_export.py
    hadoop fs -get "${hdfs_export_path}"shell_utils.sh
    hadoop fs -get "${hdfs_export_path}"config_env.sh
    hadoop fs -get "${hdfs_export_path}"sqoop_utils.py
    hadoop fs -get "${hdfs_export_path}"py_hdfs.py
    hadoop fs -get /user/dev/scratch/fake.keytab
}

setup_env_vars() {
    # Set configuration variables
    source ./config_env.sh
    source ./shell_utils.sh
    echo  "================"
    echo "Kerberos is $KERBEROS"
    echo "Running quality assurance export....."
    echo  "================"
}
do_qa() {
    echo -e " hdfs_export_path ${hdfs_export_path}"
    echo -e " python quality_assurance_export.py ${SQOOPJARS} ${source_database_name} ${source_table_name} ${target_database} ${target_table_name}  ${jdbc_url} ${connection_factories} ${user_name} ${jceks} ${password_alias} ${domain} ${target_schema} ${oozie_url} ${workflowName} "false" ${QA_EXP_RESULTS_DIR}"
    python quality_assurance_export.py "${source_database_name}" "${source_table_name}" "${target_database}" "${target_table_name}" "${SQOOPJARS}" "${jdbc_url}" "${connection_factories}" "${user_name}" "${jceks}" "${password_alias}" "${domain}" "${target_schema}" "${oozie_url}" "${workflowName}" "false" "${QA_EXP_RESULTS_DIR}"
    let EXIT_STATUS_PY_QA=$?
    echo -e " exit status ${EXIT_STATUS_PY_QA}"

    if [ "$EXIT_STATUS_PY_QA" -gt 0 ]
    then
        echo -e "------> QA Failed.\n"

        if [[ -s 'qa.log' ]] ; then
            echo -e 'Printing qa.log\n'
            cat qa.log
        else
            echo -e "Run the query: select * from ibis.qa_export_results order by log_time desc\n"
        fi
       
      fi
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

if [ "${EXIT_STATUS_PY_QA}" -gt 0 ]
then
    exit 1
fi