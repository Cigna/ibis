#!/bin/bash
# runs quality_assurance_export.py script

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

if [ -z ${hdfs_export_path+x} ]; then
    # if the workflows doesnt have hdfs_export_path env variable.
    hdfs_export_path="/user/dev/oozie/workspaces/ibis/lib/ingest/"
fi

fetch_hdfs_files() {
    hadoop fs -get "${hdfs_export_path}"impala_utils.py
    hadoop fs -get "${hdfs_export_path}"sql_queries.py
    hadoop fs -get "${hdfs_export_path}"pre_quality_assurance_export.py
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

do_check() {
    export HADOOP_CREDSTORE_PASSWORD="none"

    query_eval="select count(*) from ${database}.${target_table}"
    echo "${query_eval}"

    is_jceks=$(echo ${jceks} | awk '{print match($0,"jceks")}')

    if [ ${is_jceks} == 0 ]; then
        echo "Password file"
        echo "sqoop eval -libjars --verbose --driver ${connection_factories} --connect ${jdbc_url} --query ${query_eval} --username ${user_name} --password-file ${password_alias}"

        sqoop_output=$(sqoop eval -libjars "${SQOOPJARS}" --verbose --driver "${connection_factories}" --connect "${jdbc_url}" --query "${query_eval}" --username "${user_name}" --password-file "${password_alias}")
        SQOOP_EXIT_STATUS=$?
    else
        echo "sqoop eval -libjars -D hadoop.security.credential.provider.path=${jceks} --verbose --driver ${connection_factories} --connect ${jdbc_url} --username ${user_name} --password-alias ${password_alias} --query ${query_eval}"

        sqoop_output=$(sqoop eval -libjars "${SQOOPJARS}" -D hadoop.security.credential.provider.path=${jceks} --verbose --driver "${connection_factories}" --connect "${jdbc_url}" --username ${user_name} --password-alias ${password_alias} --query "${query_eval}")
        SQOOP_EXIT_STATUS=$?
    fi

    if [[ "$SQOOP_EXIT_STATUS" -gt 0 ]]
    then
        echo -e "------> Sqoop query Failed.\n"
        echo ${sqoop_output}
        exit 1
    fi

    row_count=$(echo ${sqoop_output} | grep -Po "\d+")

    echo "Row count:" ${row_count: -1}

    if [[ ${row_count: -1} != 0 ]]
    then
        echo -e "------> Error: Target table is not empty. Please contact DBA to truncate the target table\n"
        exit 1
    else
        echo -e "Table is empty."
    fi
}

do_qa() {
    echo -e " hdfs_export_path ${hdfs_export_path}"
    echo "DDL check start"
    echo -e " python pre_quality_assurance_export.py ${source_database_name} ${source_table_name} ${database} ${target_table} SQOOPJARS ${jdbc_url} ${connection_factories} ${user_name} ${jceks} ${password_alias} ${target_schema} false ${QA_EXP_RESULTS_DIR}"
    python pre_quality_assurance_export.py "${source_database_name}" "${source_table_name}" "${database}" "${target_table}" "${SQOOPJARS}" "${jdbc_url}" "${connection_factories}" "${user_name}" "${jceks}" "${password_alias}" "${target_schema}" "false" "${QA_EXP_RESULTS_DIR}"
    let EXIT_STATUS_PY_QA=$?

    echo -e "Exit status: ${EXIT_STATUS_PY_QA}"

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

if [ "${EXIT_STATUS_PY_QA}" -gt 0 ]
then
    exit 1
fi
