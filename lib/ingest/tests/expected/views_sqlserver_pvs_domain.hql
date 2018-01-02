SET mapred.job.queue.name=ingestion;
SET mapreduce.map.memory.mb=8000;
SET mapreduce.reduce.memory.mb=16000;
SET hive.exec.dynamic.partition.mode=nonstrict;

DROP VIEW IF EXISTS pharmacy.fake_database_mock_table;
DROP TABLE IF EXISTS pharmacy.fake_database_mock_table;
CREATE VIEW pharmacy.fake_database_mock_table AS SELECT * FROM mock_domain.fake_database_mock_table;

DROP VIEW IF EXISTS benefits.fake_database_mock_table;
DROP TABLE IF EXISTS benefits.fake_database_mock_table;
CREATE VIEW benefits.fake_database_mock_table AS SELECT * FROM mock_domain.fake_database_mock_table;