SET mapred.job.queue.name=fake_group;
SET mapreduce.map.memory.mb=8000;
SET mapreduce.reduce.memory.mb=16000;

DROP VIEW IF EXISTS fake_view_open.fake_database_fake_prog_tablename;
DROP TABLE IF EXISTS fake_view_open.fake_database_fake_prog_tablename;

CREATE DATABASE IF NOT EXISTS fake_view_open;
CREATE  TABLE fake_view_open.fake_database_fake_prog_tablename like fake_domain.fake_database_fake_prog_tablename
INSERT OVERWRITE TABLE fake_view_open.fake_database_fake_prog_tablename PARTITION(incr_ingest_timestamp) select * from fake_domain.fake_database_fake_prog_tablename ;

msck repair table fake_view_open.fake_database_fake_prog_tablename;
