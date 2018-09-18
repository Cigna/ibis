#!/bin/bash
export QUEUE_NAME=ingestion
export KERBEROS=fake.kerberos
export SQOOP_TD_CONNECTOR=sqoop-connector-teradata-1.5c5.jar
export IBIS_ENV=PROD
export KERBEROS_PRINCIPAL=_HOST@fake.domain
export IMPALA_HOST=fake.impala
export hive2_host=fake.hive
export hive2_jdbc_url=jdbc:hive2://fake.hive:25006/default
export zookeeper_hosts=fake.zookeeper1,fake.zookeeper2,fake.zookeeper3
export oozie_url=http://fake.oozie:25007/oozie/v2/

export QA_RESULTS_DIR=/user/hive/warehouse/ibis.db/qa_resultsv2
export QA_EXP_RESULTS_DIR=/user/hive/warehouse/ibis.db/qa_export_results
export CHK_BAL_DIR=/user/dev/data/ibis/checks_balances_new
export CHK_BAL_EXP_DIR=/user/dev/data/checks_balances_export
export CHK_BAL_AUDIT_DIR=/user/dev/data/checks_balances_audit
export EXPORT_HDFS_ROOT=/ibis/outbound/export/
export DOMAIN_LIST=domain1,domain2,domain3
