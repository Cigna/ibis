#!/bin/bash

# hadoop fs get fails if the file exists in current directory, so create a fresh dir everytime
RAND_DIR=$(cat /dev/urandom | head -c 2000 | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
RAND_DIR=oozie_cb_"${RAND_DIR}"
echo "$RAND_DIR"

echo "$USER"
hostname -v

mkdir $RAND_DIR
cd $RAND_DIR

fetch_hdfs_files() {
  hadoop fs -get "$hdfs_ingest_path"config_env.sh
  hadoop fs -get "$hdfs_ingest_path"shell_utils.sh
  hadoop fs -get "$hdfs_ingest_path"impala_utils.py
  hadoop fs -get "$hdfs_ingest_path"import_prep.py
  hadoop fs -get /user/dev/scratch/fake.keytab
}

setup_env_vars() {
  if [ -z ${hdfs_ingest_path+x} ]; then
    # if the workflows doesnt have hdfs_ingest_path env variable.
    hdfs_ingest_path="/user/dev/oozie/workspaces/ibis/lib/ingest/"
  fi

  # Set configuration variables
  export HADOOP_OPTS="-Dmapreduce.job.credentials.binary=$HADOOP_TOKEN_FILE_LOCATION"

  source ./config_env.sh
  source ./shell_utils.sh
  echo  "=========================="
  echo "Kerberos is $KERBEROS"
  echo "Hadoop Classpath $HADOOP_CLASSPATH"
  echo "Sqoop jars $SQOOPJARS"
  echo "Impala host $IMPALA_HOST"
  echo "Running import prep....."
  echo  "=========================="
}

main() {
  fetch_hdfs_files
  setup_env_vars
  setup_kinit
  setup_venv
  cache_table_props
  setup_it_table_env
  change_repull_status_to_inprogress
  deactivate
}

main

# WRITE CODE ABOVE THIS LINE
# WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING

cd ..
rm -rf $RAND_DIR