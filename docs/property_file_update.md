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
|[Database]|||
|host=fake.impala|Y|Update with the Imapla host|
|port=25003|Y|Port number|
|use_kerberos=True|||
|it_table=ibis.prod_it_table|N|Created by the env setup shell|
|it_table_export=ibis.prod_it_table_export|N|Created by the env setup shell|
|staging_database=fake_staging_datbase|N|Created by the env setup shell|
|checks_balances=ibis.checks_balances|N|Created by the env setup shell|
|esp_ids_table=ibis.esp_ids|N|Created by the env setup shell|
|staging_it_table=ibis.staging_it_table|N|Created by the env setup shell|
|prod_it_table=ibis.prod_it_table|N|Created by the env setup shell|
|queue_name=ingestion|||-
|edge_node=fake.edgenode|Y|Edge node address where the IBIS is installed|
|freq_ingest=ibis.freq_ingest|N|Created by the env setup shell|
|[Workflows]|||
|workflow_host=fake.impala|Y|Update with the Impala host|
|workflow_hive2_jdbc_url=jdbc:hive2://fake.hive:25006/default|Y|Update with the Hive jdbc URL|
|git_workflows_dir=PROD|||-
|kerberos=fake.kerberos|||
|db_env=PROD|N||
|hdfs_ingest_version=v14|||-
|[Directories]|||
|logs=/logs/|N||
|files=/files/|N||
|saves=/opt/app/workflows/|N||
|git_wf_local_dir=/workflows/workflows-git/|N||
|requests_dir=/opt/app/ibis/requestFiles/|N||
|root_hdfs_saves=mdm|N||
|export_hdfs_root=/ibis/outbound/export/|N||
|custom_scripts_shell=/user/dev/oozie/workspaces/ibis/shell|N||
|custom_scripts_hql=/user/dev/oozie/workspaces/ibis/hql|N||
|hdfs=/user/fake_opendev/fake_open_framework/|||-
|hdfsmodel=/user/fake_opendev/fake_open_models/|||
|workdir=/opt/app/fake_open/fake_open_framework/job_control/dynamic_wf/work/|||
|kite_shell_dir=/user/dev/oozie/workspaces/ibis/lib/ingest/kite.sh#kite.sh|N||
|kite_shell_name=kite.sh|N||
|[Templates]|||
|start_workflow=start.xml.mako|||
|end_workflow=end.xml.mako|||
|export_end_workflow=export_end.xml.mako|||
|korn_shell=esp_template.ksh.mako|||
|job_properties=prod_job.properties|||
|sub_workflow=subworkflow.xml|||
|export_to_td=export_to_td.xml|||
|fake_end_workflow=fake_end.xml|||
|[Mappers]|||-
|oracle_mappers=100:5,010:20,001:50|||
|teradata_mappers=100:2,010:5,001:8|||
|db2_mappers=100:2,010:15,001:20|||
|sqlserver_mappers=100:2,010:15,001:20|||
|mysql_mappers=100:2,010:15,001:20|||
|postgresql_mappers=100:2,010:15,001:20|||
|[Oozie]|||
|oozie_url=http://fake.oozie:25007/oozie/v2/|Y|Update with the Oozie URL|
|workspace=/user/dev/oozie/workspaces/ibis/workflows/|N||
|hadoop_credstore_password_disable=False|||
|hql_workspace=/user/dev/oozie/workspaces/hive-adhoc|N||
|hql_views_workspace=/user/dev/oozie/workspaces/ibis/hql|N||
|shell_workspace=/user/dev/oozie/workspaces/shell-adhoc|N||
|impala_workspace=/user/dev/oozie/workspaces/impala-adhoc|N||
|[ESP_ID]|||
|big_data=FAKE|Y|Update the ESP ID's initial 4 letter example "GDBD"|
|frequencies_map=daily:D,biweekly:B,weekly:W,fortnightly:F,monthly:M,quarterly:Q,adhoc:A,onetime:O,mul-appls:X,yearly:Y|N||
|environment_map=6|||
|from_branch=prod|N||
|[Other]|||
|allowed_frequencies=000:none,101:daily,011:biweekly,100:weekly,110:fortnightly,010:monthly,001:quarterly,111:yearly|N||
|vizoozie=vizoozie.properties|N||
|git_workflows_url=git@fake.git:fake_teamname/ibis-workflows.git|Y|Update with the Git workflow URL|
|git_requests_url=git@fake.git:fake_teamname/ibis-requests.gitY|Update with the Git request file URL|
|max_table_per_workflow=5|||
|sas_server=fake.sas.server|||
|sas_command=exec bash /home/fake_open/scripts/fake_openSAS.sh|||-
|parallel_dryrun_procs=25|||
|parallel_sqoop_procs=40|||
|domain_suffix=_i|||
|domains_list=pharmacy,client,customer,portal,logs,member,audit,call,claim,clinic,structure,provider,benefits|||
|tearadata_server=fake.teradata:fake,fake.teradata2:fake,fake.teradata3:fake,fake.teradata4:fake|||
