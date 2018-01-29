```
 ▄█  ▀█████████▄   ▄█     ▄████████
███    ███    ███ ███    ███    ███
███▌   ███    ███ ███▌   ███    █▀
███▌  ▄███▄▄▄██▀  ███▌   ███
███▌ ▀▀███▀▀▀██▄  ███▌ ▀███████████
███    ███    ██▄ ███           ███
███    ███    ███ ███     ▄█    ███
█▀   ▄█████████▀  █▀    ▄████████▀

```

Following are the list of property to be updated

| Property | Update Required  | Description  |
| :-   | :- | :- |
|export QUEUE_NAME=ingestion|Y|Update with HDFS queue name|
|export KERBEROS=INTERNAL.CIGNA|Y|Realm entry in krb5.conf|
|export SQOOP_TD_CONNECTOR=sqoop-connector-teradata-1.5c5.jar|Y|Update to corresponding sqoop connector Jar for Teradata|
|export IBIS_ENV=PROD|N|Default IBIS execution Environment|
|export KERBEROS_PRINCIPAL=_HOST@HADOOP.SYS.CIGNA.COM|Y|Update with the kerberos principal|
|export IMPALA_HOST=impala.sys.cigna.com|Y|Update with Impala host name|
|export hive2_host=hive.sys.cigna.com|Y|Update with Hive host name|
|export hive2_jdbc_url=jdbc:hive2://hive.sys.cigna.com:25006/default|Y|Update with Hive jdbc URL|
|export zookeeper_hosts=cilhdnmp0201.sys.cigna.com,cilhdnmp0102.sys.cigna.com,cilhdnmp0101.sys.cigna.com|Y|Update with Zookeper host name|
|export oozie_url=http://oozie.sys.cigna.com:25007/oozie/v2/|Y|Update with Oozie URL|
|export QA_RESULTS_DIR=/user/hive/warehouse/ibis.db/qa_resultsv2|N|Table directory to store QA results for Import|
|export QA_EXP_RESULTS_DIR=/user/hive/warehouse/ibis.db/qa_export_results|N|Table directory to store QA results for Export|
|export CHK_BAL_DIR=/user/dev/data/ibis/checks_balances_new|N|Table directory to store Checks and balances for the ingest load|
|export CHK_BAL_EXP_DIR=/user/dev/data/checks_balances_export|N|Table directory to store Checks and balances for the Export load|
|export CHK_BAL_AUDIT_DIR=/user/dev/data/checks_balances_audit|N|Table directory to store Checks and balances Audit for the import load|
|export EXPORT_HDFS_ROOT=/ibis/outbound/export/|n|Export directory for the table|
|export DOMAIN_LIST=pharmacy,client,customer,portal,logs,member,audit,call,claim,clinic,structure,provider,benefits|Y|List of domains to store the ingested table|
|export IT_TABLE_DIR=/user/dev/data/ibis/prod_it_table|N|Table directory to store IT table entry|
|export IT_TABLE_COLS="list of columns"|N|List of ibis columns|

