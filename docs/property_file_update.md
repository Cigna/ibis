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
|**[Database]**|||
|host=fake.impala|Y|Update with the Impala host|
|port=25003|Y|Port number|
|use_kerberos=True|Y|If kerberos is enabled "True" otherwise "False"|
|it_table=ibis.prod_it_table|N|Created by the ibis setup shell|
|it_table_export=ibis.prod_it_table_export|N|Created by the ibis setup shell|
|staging_database=fake_staging_datbase|N|Created by the ibis setup shell|
|checks_balances=ibis.checks_balances|N|Created by the ibis setup shell|
|esp_ids_table=ibis.esp_ids|N|Created by the ibis setup shell|
|staging_it_table=ibis.staging_it_table|N|Created by the ibis setup shell|
|prod_it_table=ibis.prod_it_table|N|Created by the ibis setup shell|
|queue_name=ingestion|Y|Update with HDFS queue name|
|edge_node=fake.edgenode|Y|Edge node address where the IBIS is installed|
|freq_ingest=ibis.freq_ingest|N|Created by the ibis setup shell|
|**[Workflows]**|||
|workflow_host=fake.impala|Y|Update with the Impala host|
|workflow_hive2_jdbc_url=jdbc:hive2://fake.hive:25006/default|Y|Update with the Hive jdbc URL|
|git_workflows_dir=PROD|N|prod git's workflows directory |-
|kerberos=fake.kerberos|Y|Realm entry in krb5.conf |
|db_env=PROD|N||
|hdfs_ingest_version=v14|Y|Increment this version whenever there is change in XML|
|**[Directories]**|||
|logs=/logs/|N||
|files=/files/|N||
|saves=/opt/app/workflows/|N||
|git_wf_local_dir=/workflows/workflows-git/|N||
|requests_dir=/opt/app/ibis/requestFiles/|N||
|root_hdfs_saves=mdm|N||
|export_hdfs_root=/ibis/outbound/export/|N||
|custom_scripts_shell=/user/dev/oozie/workspaces/ibis/shell|N||
|custom_scripts_hql=/user/dev/oozie/workspaces/ibis/hql|N||
|hdfs=/user/fake_opendev/fake_open_framework/|Matt to confirm|Specific to Opensae|
|hdfsmodel=/user/fake_opendev/fake_open_models/|Matt to confirm|Specific to Opensae|
|workdir=/opt/app/fake_open/fake_open_framework/job_control/dynamic_wf/work/|Matt to confirm|Specific to Opensae|
|kite_shell_dir=/user/dev/oozie/workspaces/ibis/lib/ingest/kite.sh#kite.sh|N||
|kite_shell_name=kite.sh|N||
|**[Templates]**|N||
|start_workflow=start.xml.mako|N||
|end_workflow=end.xml.mako|N||
|export_end_workflow=export_end.xml.mako|N||
|korn_shell=esp_template.ksh.mako|N||
|job_properties=prod_job.properties|N||
|sub_workflow=subworkflow.xml|N||
|export_to_td=export_to_td.xml|N||
|fake_end_workflow=fake_end.xml|N||
|**[Mappers]**|||-
|oracle_mappers=100:5,010:20,001:50|N|100:5,010:20,001:50 --> Size of table:Number of mappers |
|teradata_mappers=100:2,010:5,001:8|N|Refer [table's weight parameter](docs/ibis_features.md) in request file for translation|
|db2_mappers=100:2,010:15,001:20|N|Number of mappers can be updated, is an optional update as per the requirement |
|sqlserver_mappers=100:2,010:15,001:20|N||
|mysql_mappers=100:2,010:15,001:20|N||
|postgresql_mappers=100:2,010:15,001:20|N||
|**[Oozie]**|||
|oozie_url=http://fake.oozie:25007/oozie/v2/|Y|Update with the Oozie URL|
|workspace=/user/dev/oozie/workspaces/ibis/workflows/|N||
|hadoop_credstore_password_disable=False|Y|It should be "False" if jceks is used for storing password|
|hql_workspace=/user/dev/oozie/workspaces/hive-adhoc|N||
|hql_views_workspace=/user/dev/oozie/workspaces/ibis/hql|N||
|shell_workspace=/user/dev/oozie/workspaces/shell-adhoc|N||
|impala_workspace=/user/dev/oozie/workspaces/impala-adhoc|N||
|**[ESP_ID]**|||
|big_data=FAKE|Y|Update the ESP ID's first 4 letter's for example "GDBD" in Appl ID : GDBDD006|
|frequencies_map=daily:D,biweekly:B,weekly:W,fortnightly:F,monthly:M,quarterly:Q,adhoc:A,onetime:O,mul-appls:X,yearly:Y|N||
|environment_map=6|Y|Is an optional update. Its the last digit of the Appl ID used to identify the env in which ESP Appl is running, in this case '6' will be suffixed in the Appl ID : GDBDD006|
|from_branch=prod|N||
|**[Other]**|||
|allowed_frequencies=000:none,101:daily,011:biweekly,100:weekly,110:fortnightly,010:monthly,001:quarterly,111:yearly|N||
|vizoozie=vizoozie.properties|N||
|git_workflows_url=git@fake.git:fake_teamname/ibis-workflows.git|Y|Update with the Git workflow URL|
|git_requests_url=git@fake.git:fake_teamname/ibis-requests.git|Y|Update with the Git request file URL|
|max_table_per_workflow=5|Y|Maximum number of table per oozie workflow|
|sas_server=fake.sas.server|Matt to confirm|Specific to Opensae|
|sas_command=exec bash /home/fake_open/scripts/fake_openSAS.sh|Matt to confirm|Specific to Opensae|
|parallel_dryrun_procs=25|N|Oozie XML dryrun or test. Is an optional update field|
|parallel_sqoop_procs=40|N|Number of parallel sqoop processes. Is an optional update field|
|domain_suffix=_i|N|Suffixed to the domain(sqoop import master) database|
|domains_list=pharmacy,client,customer,portal,logs,member,audit,call,claim,clinic,structure,provider,benefits|Y|Refer [table's views parameter](docs/ibis_features.md) in request file|
|tearadata_server=fake.teradata:fake,fake.teradata2:fake,fake.teradata3:fake,fake.teradata4:fake|Y|Automatic split_by for teradata. In this case "fake.teradata:fake", server and table are separated by the colon where fake is the table name in IBIS DB which holds split by information of all table's in the given server |
