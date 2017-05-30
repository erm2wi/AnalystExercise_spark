insert overwrite directory '/dualcore/spark_ad_data1/'
row format delimited fields terminated by '\t'
select * from ad_data1_converted;