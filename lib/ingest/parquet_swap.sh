#!/bin/bash

# hadoop fs get fails if the file exists in current directory, so create a fresh dir everytime
RAND_DIR=$(cat /dev/urandom | head -c 2000 | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
RAND_DIR=swap_"$RAND_DIR"

hostname -v

mkdir $RAND_DIR
cd $RAND_DIR

# WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING
# WRITE CODE BELOW THIS LINE

hadoop fs -mkdir -p /user/data/backup/${target_dir}
hadoop fs -mkdir -p /user/data/${target_dir}/live
hadoop fs -rm -r /user/data/backup/${target_dir}
hadoop fs -mv /user/data/${target_dir}/live /user/data/backup/${target_dir}
hadoop fs -rm -r /user/data/${target_dir}/live
hadoop fs -mv /user/data/${target_dir}/stage /user/data/${target_dir}/live
hadoop fs -rm -r /user/data/${target_dir}/stage
hadoop fs -chmod -R 770 /user/data/${target_dir}
hadoop fs -chown :fake_group /user/data/{$target_dir}

# Delete avro files
hadoop fs -rm -r /user/data/ingest/${target_dir}

cd ..
rm -rf $RAND_DIR