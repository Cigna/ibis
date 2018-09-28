#!/bin/bash

# hadoop fs get fails if the file exists in current directory, so create a fresh dir everytime
RAND_DIR=$(cat /dev/urandom | head -c 2000 | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
RAND_DIR=oozie_cb_"${RAND_DIR}"
echo "$RAND_DIR"

echo "$USER"
hostname -v

mkdir $RAND_DIR
cd $RAND_DIR

# WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING
# WRITE CODE BELOW THIS LINE
if [ -z ${hdfs_ingest_path+x} ]; then
    # if the workflows doesnt have hdfs_ingest_path env variable.
    hdfs_ingest_path="/user/dev/oozie/workspaces/ibis/lib/ingest/"
fi

fetch_hdfs_files() {
    hadoop fs -get /user/dev/scratch/fake.keytab
    # Set configuration variables
    hadoop fs -get "$hdfs_ingest_path"config_env.sh
    hadoop fs -get "$hdfs_ingest_path"shell_utils.sh
    hadoop fs -get "$hdfs_ingest_path"oozie_ws_helper.py
    hadoop fs -get "$hdfs_ingest_path"impala_utils.py
    hadoop fs -get "$hdfs_ingest_path"py_hdfs.py
}

setup_env_vars() {
    export HADOOP_OPTS="-Dmapreduce.job.credentials.binary=$HADOOP_TOKEN_FILE_LOCATION"
    source ./config_env.sh
    source ./shell_utils.sh
    echo "============================================="
    echo "Kerberos is $KERBEROS"
    echo "Hadoop Classpath $HADOOP_CLASSPATH"
    echo "============================================="
}

insert_oozie_stats() {
    echo -e "python oozie_ws_helper.py ${workflowName} ${oozie_url} ${CHK_BAL_DIR}"
    python oozie_ws_helper.py "${workflowName}" "${oozie_url}" "${CHK_BAL_DIR}"
    let EXIT_STATUS_PY=$?

    if [[ "$EXIT_STATUS_PY" -gt 0 ]]
    then
        echo -e "FAILED --------------> oozie_ws_helper.py\n"
    fi
}

main() {
  fetch_hdfs_files
  setup_env_vars
  setup_kinit
  setup_venv
  insert_oozie_stats
  deactivate
}

main

cd ..
rm -rf $RAND_DIR


if [[ "$EXIT_STATUS_PY" -gt 0 ]]
then
    exit 1
fi

