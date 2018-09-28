#!/bin/bash

# hadoop fs get fails if the file exists in current directory, so create a fresh dir everytime
RAND_DIR=$(cat /dev/urandom | head -c 2000 | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
RAND_DIR=kite_"$RAND_DIR"

mkdir $RAND_DIR
cd $RAND_DIR

# All files to ingest must be in the conventional HDFS location and either
# plain-text or gzip

# Conventional location is hdfs://nameservice1/ibis/inbound/kite_test/${db}/${table}/

# Standard log junk. Someday there will be Python, maybe

# Logging util functions
log_debug () {
  echo "$(date) [DEBUG] : $@" >&2
}

log_info () {
  echo "$(date) [INFO]  : $@" >&2
}

log_warn () {
  echo "$(date) [WARN]  : $@" >&2
}

log_error () {
  echo "$(date) [ERROR] : $@" >&2
}

log_fatal () {
  echo "$(date) [FATAL] : $@" >&2
}

main () {

  hadoop fs -get "$hdfs_ingest_path"shell_utils.sh
  hadoop fs -get /user/dev/scratch/fake.keytab

  source ./shell_utils.sh

  setup_kinit

  # Set args
  local readonly DB=$1
  local readonly TABLE=$2
  local readonly HDFS_LOC=$3

  # Derived variables
  local readonly INGEST_DIRECTORY="hdfs://nameservice1${HDFS_LOC}/"
  local readonly TARGET_DS_URL="dataset:hive:${DB}/${TABLE}"

  log_info "Welcome to this thing"
  log_info "Staging ingestion for ${DB}.${TABLE}"

  log_info "Beginning ingestion"
  /opt/cloudera/parcels/CDH/bin/kite-dataset -v csv-import --overwrite $INGEST_DIRECTORY $TARGET_DS_URL
}

main $@

cd ..
rm -rf $RAND_DIR