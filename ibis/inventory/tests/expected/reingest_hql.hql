SET mapred.job.queue.name=fake_group;
SET mapreduce.map.memory.mb=8000;
SET mapreduce.reduce.memory.mb=16000;

DROP VIEW IF EXISTS test_view.src_db_src_table;

DROP TABLE IF EXISTS test_view.src_db_src_table;

set hive.exec.dynamic.partition.mode=nonstrict;

CREATE DATABASE IF NOT EXISTS test_view;

CREATE  TABLE test_view.src_db_src_table like full_table;

INSERT OVERWRITE TABLE test_view.src_db_src_table PARTITION(incr_ingest_timestamp) select * from full_table ;