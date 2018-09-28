#!/bin/bash

# hadoop fs get fails if the file exists in current directory, so create a fresh dir everytime
RAND_DIR=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
RAND_DIR=oozie_cb_"${RAND_DIR}"
echo "$RAND_DIR"

echo "$USER"
hostname -v

mkdir $RAND_DIR
cd $RAND_DIR

# WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING
# WRITE CODE BELOW THIS LINE
if [ -z ${hdfs_export_path+x} ]; then
    # if the workflows doesnt have hdfs_export_path env variable.
    hdfs_export_path="/user/dev/oozie/workspaces/ibis/lib/ingest/"
fi

fetch_hdfs_files() {
    hadoop fs -get /user/dev/scratch/fake.keytab
    # Set configuration variables
    hadoop fs -get "$hdfs_export_path"config_env.sh
    hadoop fs -get "$hdfs_export_path"shell_utils.sh
    hadoop fs -get "$hdfs_export_path"impala_utils.py
    hadoop fs -get "$hdfs_export_path"checks_and_balances_export.py
    hadoop fs -get "$hdfs_export_path"py_hdfs.py
}

setup_env_vars() {
    export HADOOP_OPTS="-Dmapreduce.job.credentials.binary=$HADOOP_TOKEN_FILE_LOCATION"
    source ./config_env.sh
    source ./shell_utils.sh
    echo "============================================="
    echo "Kerberos is $KERBEROS"
    echo "Hadoop Classpath $HADOOP_CLASSPATH"
    echo "============================================="

    oozie_url=$(
      case "$IBIS_ENV" in
         ("DEV") echo "http://fake.dev.oozie:25007/oozie/v2/" ;;
         ("INT") echo "http://fake.int.oozie:25007/oozie/v2/" ;;
         ("PROD") echo "http://fake.oozie:25007/oozie/v2/" ;;
         (*) echo "unknown_host";;
      esac)
}

setup_kinit() {
    # kinit
    kinit -S krbtgt/${KERBEROS}.COM@${KERBEROS}.COM fake_username@${KERBEROS}.COM -k -t fake.keytab
}

insert_oozie_stats() {
    echo -e "python checks_and_balances_export.py ${workflowName} ${oozie_url} ${CHK_BAL_EXP_DIR}"
    python checks_and_balances_export.py "${workflowName}" "${oozie_url}" "${CHK_BAL_EXP_DIR}"
    let EXIT_STATUS_PY=$?

    if [[ "$EXIT_STATUS_PY" -gt 0 ]]
    then
        echo -e "FAILED ------------> checks_and_balances_export.py\n"
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

