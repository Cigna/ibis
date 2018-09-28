SET mapred.job.queue.name=fake_group;
SET mapreduce.map.memory.mb=8000;
SET mapreduce.reduce.memory.mb=16000;

DROP VIEW IF EXISTS fake_view_open.fake_database_fake_prog_tablename;
DROP TABLE IF EXISTS fake_view_open.fake_database_fake_prog_tablename;

CREATE DATABASE IF NOT EXISTS fake_view_open;
CREATE DATABASE IF NOT EXISTS client;
DROP VIEW IF EXISTS client.fake_database_fake_prog_tablename;
DROP TABLE IF EXISTS client.fake_database_fake_prog_tablename;
CREATE VIEW client.fake_database_fake_prog_tablename AS SELECT * FROM fake_domainfake_database_fake_prog_tablename;
 CREATE  TABLE fake_view_open.fake_database_fake_prog_tablename like fake_domainfake_database_fake_prog_tablename
INSERT OVERWRITE TABLE fake_view_open.fake_database_fake_prog_tablename PARTITION(incr_ingest_timestamp) select * from client.fake_database_fake_prog_tablename ;

msck repair table fake_view_open.fake_database_fake_prog_tablename;
