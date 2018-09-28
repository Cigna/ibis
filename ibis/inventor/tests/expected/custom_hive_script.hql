SET mapred.job.queue.name=fake_group;
SET mapreduce.map.memory.mb=8000;
SET mapreduce.reduce.memory.mb=16000;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET hive.warehouse.subdir.inherit.perms=true;

SET parquet.block.size=268435456;
SET dfs.blocksize=1000000000;
SET mapred.min.split.size=1000000000;


use ibis;
