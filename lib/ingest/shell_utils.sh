#!/bin/bash

synchronize() {
    # Args: pass the command or method that needs to be synchronized Ex: synchronize "python hello.py"
    # code which can affect shared directories like /tmp need to be synchronized
    t1=`date +%s`
    exec 7>/tmp/ibis_lock3.txt
    (
    flock -x 7
    echo locked
    $1
    )7>/tmp/ibis_lock3.txt
    echo unlocked
    t2=`date +%s`
    echo Seconds: `expr $t2 - $t1`
}

setup_venv() {
    # Get venv
    hadoop fs -get /user/dev/scratch/ibis_venv.tar.gz
    tar xfz ibis_venv.tar.gz
    curr_dir=`pwd`
    export LD_LIBRARY_PATH=${curr_dir}/ibis_venv/ld_library
    # export LD_LIBRARY_PATH=/usr/local/lib/
    convert_txt="/VIRTUAL_ENV=/c\VIRTUAL_ENV=${curr_dir}/ibis_venv"
    sed -i ${convert_txt} ibis_venv/bin/activate
    source ibis_venv/bin/activate
    echo -e 'python venv activated'
}

setup_it_table_env() {
    hadoop fs -get /user/data/${target_dir}/it_table/it_table_env.sh
    source ./it_table_env.sh
    setup_clean_table_name
}

cache_table_props() {
    if [ -z ${db_env+x} ]; then
        # if the workflow doesnt have db_env env variable.
        db_env=$(echo "${IBIS_ENV}" | awk '{print tolower($0)}')
    fi
    # we cache the current table entry from it-table to HDFS and retrieve it in
    # further actions of the workflow
    hadoop fs -mkdir -p /user/data/${target_dir}/it_table/
    python import_prep.py "${source_database_name}" "${source_table_name}" "${db_env}" "${it_table}" "${it_table_host}"
    let EXIT_STATUS_PY=$?

    if [ "$EXIT_STATUS_PY" -gt 0 ]
    then
        echo -e "FAILED ----------> cache_table_props - import_prep.py\n"
        exit 1
    else
        hadoop fs -put -f it_table_env.sh /user/data/${target_dir}/it_table/
        let EXIT_STATUS=$?
        if [ "$EXIT_STATUS" -gt 0 ]
        then
            echo -e "FAILED ----------> Hadoop put failed - shell_utils.sh\n"
            exit 1
        else
            echo -e "cached it_table!"
        fi
    fi
}

setup_sqoop_env() {
    # All the JAR files are in the container's dir, so we no longer
    # need to pull down the JARS ourself

    # Get the dir above the RAND_DIR that we create
    PARENT_DIR=`dirname $PWD`

    # Used as -libjars for remote map/reduce tasks, seperated by commoa (,)
    export SQOOPJARS=`echo ${PARENT_DIR}/*.jar | sed s/\ /,/g`
    # Needed for sqoop-eval statements, client jvm, seperated by colon (:)
    export HADOOP_CLASSPATH=`echo ${PARENT_DIR}/*.jar | sed s/\ /:/g`
}

setup_kinit() {
    kinit -S krbtgt/${KERBEROS}.COM@${KERBEROS}.COM fake_username@${KERBEROS}.COM -k -t fake.keytab
}


upload_view_files() {
    # Views: copy external table query to hdfs
    if [[ -s "views.hql" ]] ; then
        hadoop fs -put -f views.hql /user/data/${target_dir}/gen/
        echo -e "views external table uploaded"
    fi

    if [[ -s "views_invalidate.txt" ]] ; then
        hadoop fs -put -f views_invalidate.txt /user/data/${target_dir}/gen/
        echo -e "table and views invalidate uploaded"
    fi

    if [[ -s "views_info.txt" ]] ; then
        hadoop fs -put -f views_info.txt /user/data/${target_dir}/gen/
        echo -e "table and views info uploaded"
    fi
}

change_repull_status_to_inprogress() {
    ## Below query sets the current re-pull flag to '1' in the checks_balances row
    ## and null's out all the other columns
    ingest_timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    move_row_to_audit
    hadoop fs -mkdir -p /tmp/domain=${domain}/table=${source_database_name}_${source_table_name}/
    hadoop fs -mv ${CHK_BAL_DIR}/domain=${domain}/table=${source_database_name}_${source_table_name}/* /tmp/domain=${domain}/table=${source_database_name}_${source_table_name}/
    hadoop fs -mkdir -p ${CHK_BAL_DIR}/domain=${domain}/table=${source_database_name}_${source_table_name}/
    # change repull to 1-in_progress and later in oozie_cb action it can be set to 0-success or 2-failed
    filename="FBO_$RAND_DIR"
    echo -e "|||${ingest_timestamp}|||||||1|" > ${filename}
    hadoop fs -put -f ${filename} ${CHK_BAL_DIR}/domain=${domain}/table=${source_database_name}_${source_table_name}/

    # Make sure to execute INVALIDATE METADATA to reflect the underlying data changes in Impala queries.
    python impala_utils.py "ibis.checks_balances" "SELECT 1=1" "select" "impala"

    let EXIT_STATUS_PY=$?
    if [ "$EXIT_STATUS_PY" -gt 0 ]
    then
      echo -e "FAILED ----------> change_repull_status_to_inprogress \n"
      exit 1
    fi
    # do msck repair
    hiveQuery=`beeline /etc/hive/beeline.properties -u ${hive2_jdbc_url}\;principal=hive/${KERBEROS_PRINCIPAL} --hiveconf mapred.job.queue.name=$queueName --silent=true --showHeader=false --outputformat=csv2 -e "msck repair table ibis.checks_balances;"`
    # do msck repair
    hiveQuery=`beeline /etc/hive/beeline.properties -u ${hive2_jdbc_url}\;principal=hive/${KERBEROS_PRINCIPAL} --hiveconf mapred.job.queue.name=$queueName --silent=true --showHeader=false --outputformat=csv2 -e "msck repair table ibis.checks_balances_audit;"`
}

move_row_to_audit() {
    ## Below query moves the current checks_balance row into the audit table,
    ## if it exists. Otherwise, it'll be a no-op.
    hadoop fs -test -e ${CHK_BAL_DIR}/domain=${domain}/table=${source_database_name}_${source_table_name}/*
    let EXIT_STATUS=$?

    if [ "${EXIT_STATUS}" -eq 0 ]
    then
        # File-based insertion into Hive table
        filename="FBO_$RAND_DIR"
        echo -e "hadoop fs -cat ${CHK_BAL_DIR}/domain=${domain}/table=${source_database_name}_${source_table_name}/* | hadoop fs -appendToFile - ${CHK_BAL_AUDIT_DIR}/${filename}"
        hadoop fs -cat ${CHK_BAL_DIR}/domain=${domain}/table=${source_database_name}_${source_table_name}/* | hadoop fs -appendToFile - ${CHK_BAL_AUDIT_DIR}/${filename}

        let EXIT_STATUS_PY=$?
        if [ "$EXIT_STATUS_PY" -gt 0 ]
        then
            echo -e "FAILED ----------> move_row_to_audit \n"
            exit 1
        fi
    fi
}

remove_locks() {
    python zookeeper_remove_locks.py views_info.txt ${zookeeper_hosts}

    let EXIT_STATUS_PY=$?

    if [ "${EXIT_STATUS_PY}" -gt 0 ]
    then
        echo -e "FAILED ----------> zookeeper_remove_locks.py\n"
    fi
}

clean_name() {
    # replaces any char other than alphnumeric and underscore to empty
    echo "$1" | awk '{gsub(/[^A-Za-z0-9_]/, "", $0); print}'
}

setup_clean_table_name() {
    clean_domain=$(clean_name ${domain})
    clean_database=$(clean_name ${source_database_name})
    clean_table_name=$(clean_name ${source_table_name})
    clean_full_name="${clean_domain}.${clean_database}_${clean_table_name}"
}