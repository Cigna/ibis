SET mapred.job.queue.name=ingestion;
set hive.warehouse.subdir.inherit.perms=true;

create database if not exists `ingest`;

drop table if exists `ingest.fake_database_fake_tablename`;

create external table `ingest.fake_database_fake_tablename` row format serde 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' location 'hdfs:///user/data/ingest/mock_hdfs_loc' tblproperties ('avro.schema.url'='hdfs:///user/data/ingest/mock_hdfs_loc/_gen/fake_$tablename.avsc');

