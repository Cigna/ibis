SET mapred.job.queue.name=ingestion;
SET mapreduce.map.memory.mb=8000;
SET mapreduce.reduce.memory.mb=16000;

DROP VIEW IF EXISTS `fake_view_open.fake_database_mock_table`;
DROP TABLE IF EXISTS `fake_view_open.fake_database_mock_table`;

CREATE DATABASE IF NOT EXISTS `fake_view_open`;
CREATE EXTERNAL TABLE `fake_view_open.fake_database_mock_table` (mock_create_hql,
 `ingest_timestamp` string)
 partitioned by (incr_ingest_timestamp string)
 stored as parquet location 'hdfs:///user/data/mock_hdfs_loc/live/';

msck repair table `fake_view_open.fake_database_mock_table`;

DROP VIEW IF EXISTS `fake_view_im.fake_database_mock_table`;
DROP TABLE IF EXISTS `fake_view_im.fake_database_mock_table`;

CREATE DATABASE IF NOT EXISTS `fake_view_im`;

CREATE EXTERNAL TABLE `fake_view_im.fake_database_mock_table` (mock_create_hql,
`ingest_timestamp` string)
 partitioned by (incr_ingest_timestamp string)
stored as parquet location 'hdfs:///user/data/mock_hdfs_loc/live/';

msck repair table `fake_view_im.fake_database_mock_table`;
