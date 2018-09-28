SET mapred.job.queue.name=ingestion;
SET mapreduce.map.memory.mb=8000;
SET mapreduce.reduce.memory.mb=16000;
set hive.warehouse.subdir.inherit.perms=true;

create database if not exists `mock_domain`;

drop table if exists `mock_domain.fake_database_fake_tablename`;

create external table `mock_domain.fake_database_fake_tablename` like `parquet_stage.fake_database_fake_tablename` location 'hdfs:///user/data/mock_hdfs_loc/live/';

alter table `mock_domain.fake_database_fake_tablename` add partition (incr_ingest_timestamp='full_20160101164756');

drop table if exists `parquet_stage.fake_database_fake_tablename`;
drop table if exists `ingest.fake_database_fake_tablename`;
