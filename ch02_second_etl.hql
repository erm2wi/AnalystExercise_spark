insert overwrite directory '/dualcore/spark_ad_data2/'
row format delimited fields terminated by '\t'
select * from ad_data2_fixformat;