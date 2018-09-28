#!/bin/bash

# hadoop fs get fails if the file exists in current directory, so create a fresh dir everytime
RAND_DIR=$(cat /dev/urandom | head -c 2000 | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
RAND_DIR=impala_cleanup_"$RAND_DIR"
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

# Impala requires Python egg cache directory
mkdir python-eggs
export PYTHON_EGG_CACHE=python-eggs
export HADOOP_OPTS="-Dmapreduce.job.credentials.binary=$HADOOP_TOKEN_FILE_LOCATION"

# get key tab
hadoop fs -get /user/dev/scratch/fake.keytab

# Set configuration variables
hadoop fs -get "$hdfs_ingest_path"config_env.sh
hadoop fs -get "$hdfs_ingest_path"shell_utils.sh

source ./config_env.sh
source ./shell_utils.sh

setup_it_table_env

# kinit
###### DEPENDENT ON REALM ########
setup_kinit

echo  "================================"
echo "Kerberos is $KERBEROS"
echo "Hadoop Classpath $HADOOP_CLASSPATH"
echo "Running impala cleanup....."
echo  "================================"

# INVALIDATE METADATA causes the metadata for that table to be marked as stale, and reloaded the next time the table
# is referenced.

hadoop fs -get /user/data/${target_dir}/gen/views_invalidate.txt

if [[ -s "views_invalidate.txt" ]] ; then
    impala-shell -k -i ${IMPALA_HOST}:25004 -f views_invalidate.txt
    echo -e "views - impala invalidated"
fi

invalidate_query="invalidate metadata ${clean_domain}.${clean_database}_${clean_table_name}"

impala-shell -k -i ${IMPALA_HOST}:25004 -q "${invalidate_query}"

echo -e ${invalidate_query}

# Gathers information about volume and distribution of data in a table and all associated columns and partitions.
# The information is stored in the metastore database, and used by Impala to help optimize queries.
# MJW: Note: this is currently turned off due to the time it takes to complete
# impala-shell -q "COMPUTE STATS ${domain}.${database}_${table_name}"

# WRITE CODE ABOVE THIS LINE
# WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING

cd ..
rm -rf $RAND_DIR

