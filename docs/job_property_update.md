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
|nameNode=hdfs://nameservice1|N|HDFS nameservice property in hdfs-site.xml |
|jobTracker=fake.dev.jobtracker:8032|Y|Name node address where IBIS is installed|
|queueName=ingestion|Y|Update with HDFS queue name|
|oozie.use.system.libpath=true|N||
|oozie.libpath=/user/dev/oozie/share/jars/|Y|Is an optional field to update. Path where all the sqoop jars should be present |
|fs.hdfs.impl.disable.cache=true|N||
|hive2_jdbc_url=jdbc:hive2://fake.dev.hive:25006/default|Y|Update with Hive jdbc URL|
|hive2_principal=hive/fake.dev.hive@fake.domain|Y|Update with hive server principal|
|impala_daemon=fake.dev.impala:25004|Y|Update with Impala host name and port number|

