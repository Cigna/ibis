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

Following are the list of properties to be updated

| Property | Update Required  | Description  |
| :-   | :- | :- |
|**[Database]**|||
|host=fake.impala|Y|Update with the Impala host name|
|port=25003|Y|Impala port number|
|use_kerberos=True|Y|If kerberos is enabled "True" otherwise "False"|
|it_table=ibis.prod_it_table|N|Created by the ibis setup shell. Please match the table name with setup shell. Holds the entry for each table to be ingested|
|it_table_export=ibis.prod_it_table_export|N|Created by the ibis setup shell. Please match the table name with setup shell. Holds the entry for each table to be exported|
|staging_database=fake_staging_datbase|N|Created by the ibis setup shell. Please match the table name with setup shell. Temporarily Holds the data load of each table |
|checks_balances=ibis.checks_balances|N|Created by the ibis setup shell. Please match the table name with setup shell. Holds the entry for each table load|
|esp_ids_table=ibis.esp_ids|N|Created by the ibis setup shell. Please match the table name with setup shell. Stores the Appl ID and frequency details|
|staging_it_table=ibis.staging_it_table|N|Created by the ibis setup shell. Please match the table name with setup shell. Stores tables to be ingested through schedule|
|prod_it_table=ibis.prod_it_table|N|Created by the ibis setup shell. Please match the table name with setup shell. Holds the entry for each table to be ingested|
|queue_name=ingestion|Y|Update with HDFS queue name for loading the table|
|edge_node=fake.edgenode|Y|HDFS Edge node address where the IBIS workflow will be executed|
|freq_ingest=ibis.freq_ingest|N|Created by the ibis setup shell. Table for PERF run frequency check  |
|**[Workflows]**|||
|workflow_host=fake.impala|Y|Update with the Impala host name|
|workflow_hive2_jdbc_url=jdbc:hive2://fake.hive:25006/default|Y|Update with the Hive jdbc URL|
|kerberos=fake.kerberos|Y|Realm entry in krb5.conf |
|db_env=PROD|N||
|hdfs_ingest_version=v14|Y|Increment this version whenever there is change to workflow XML|
|**[Directories]**||IBIS directories for request files, hql's and workflows|
|logs=/logs/|N| Logs location|
|files=/files/|N| test files locations|
|saves=/opt/app/workflows/|N| oozie property and ksh files location|
|requests_dir=/opt/app/ibis/requestFiles/|N|Request files to generate workflow will be placed in this location|
|root_hdfs_saves=mdm|N|ingestion root directory on hdfs|
|export_hdfs_root=/ibis/outbound/export/|N|HDFS  directory on HDFS, keep files to be exported under this folder|
|custom_scripts_shell=/user/dev/oozie/workspaces/ibis/shell|N|HDFS location to keep the shells to be executed|
|custom_scripts_hql=/user/dev/oozie/workspaces/ibis/hql|N|HDFS location to keep the HQL's to be executed|
|kite_shell_dir=/user/dev/oozie/workspaces/ibis/lib/ingest/kite.sh#kite.sh|N||
|kite_shell_name=kite.sh|N||
|**[Templates]**|N||
|start_workflow=start.xml.mako|N| workflow start template|
|end_workflow=end.xml.mako|N|workflow end template|
|export_end_workflow=export_end.xml.mako|N|export workflow end template|
|korn_shell=esp_template.ksh.mako|N|workflow KSH template|
|job_properties=prod_job.properties|N|Job properties template|
|sub_workflow=subworkflow.xml|N|Sub workflow template|
|export_to_td=export_to_td.xml|N|teradata workflow template|
|fake_end_workflow=fake_end.xml|N|workflow end template|
|**[Mappers]**|||-
|oracle_mappers=100:5,010:20,001:50|N|100:5,010:20,001:50 --> Size of table:Number of mappers |
|teradata_mappers=100:2,010:5,001:8|N|Refer [table's weight parameter](/README.md) in request file for translation|
|db2_mappers=100:2,010:15,001:20|N|Number of mappers can be updated, is an optional update as per the requirement |
|sqlserver_mappers=100:2,010:15,001:20|N||
|mysql_mappers=100:2,010:15,001:20|N||
|postgresql_mappers=100:2,010:15,001:20|N||
|**[Oozie]**|||
|oozie_url=http://fake.oozie:25007/oozie/v2/|Y|Update with the Oozie URL|
|workspace=/user/dev/oozie/workspaces/ibis/workflows/|N|HDFS location for workflows to be deployed|
|hadoop_credstore_password_disable=False|Y|It should be "False" if jceks is used for storing password|
|hql_workspace=/user/dev/oozie/workspaces/hive-adhoc|N|HDFS location for HQL's by team to be deployed||
|hql_views_workspace=/user/dev/oozie/workspaces/ibis/hql|N|HDFS location for HQl to be deployed||
|shell_workspace=/user/dev/oozie/workspaces/shell-adhoc|N|HDFS location for shells to be deployed||
|impala_workspace=/user/dev/oozie/workspaces/impala-adhoc|N|HDFS location for impala-scripts to be deployed||
|**[ESP_ID]**|||
|big_data=FAKE|Y|Update the ESP ID's first 4 letter's for example "GDBD" in Appl ID : GDBDD006|
|frequencies_map=daily:D,biweekly:B,weekly:W,fortnightly:F,monthly:M,quarterly:Q,adhoc:A,onetime:O,mul-appls:X,yearly:Y|N|First letter of frequency is used in the Appl ID creations and its the letter in Appl ID|
|environment_map=6|Y|Is an optional update. Its the last digit of the Appl ID used to identify the env in which ESP Appl is running, in this case '6' will be suffixed in the Appl ID : GDBDD006|
|from_branch=prod|N||
|**[Other]**|||
|allowed_frequencies=000:none,101:daily,011:biweekly,100:weekly,110:fortnightly,010:monthly,001:quarterly,111:yearly|N|All allowed frequencies for scheduling workflows|
|vizoozie=vizoozie.properties|N|Property for all wokflows PDF's generated|
|max_table_per_workflow=5|Y|Maximum number of table per oozie workflow|
|parallel_dryrun_procs=25|N|Oozie XML dryrun or test. Is an optional update field|
|parallel_sqoop_procs=40|N|Number of parallel sqoop processes. Is an optional update field|
|domain_suffix=_i|N|Suffixed to the domain(sqoop import master) database|
|domains_list=domain1,domain2,domain3|Y|Refer [table's views parameter](/README.md) in request file|
|teradata_server=fake.teradata:fake,fake.teradata2:fake,fake.teradata3:fake,fake.teradata4:fake|Y|Automatic split_by for teradata. In this case "fake.teradata:fake", server and table are separated by the colon where fake is the table name in IBIS DB which holds split by information of all table's in the given server |
