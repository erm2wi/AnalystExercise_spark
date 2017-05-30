val ad_data2=sqlContext.read.table("ad_data2")
ad_data2.persist
ad_data2.count
val ad_data2_deduped=ad_data2.distinct
ad_data2_deduped.count
val ad_data2_reorder = ad_data2_deduped.selectExpr("campaign_id","date","time","trim(upper(keyword)) as keyword","display_site","placement","was_clicked","cpc")
//ad_data2_reorder.withColumn("new_date",regexp_replace("date","-","/"))  --not work, expect a column 
//ad_data2_reorder.withColumn("new_date",regexp_replace(ad_data2_reorder("date"),"-","/"))
ad_data2_reorder.withColumn("date",regexp_replace(ad_data2_reorder("date"),"-","/"))
val ad_data2_fixformat = ad_data2_reorder.withColumn("date",regexp_replace(ad_data2_reorder("date"),"-","/"))
ad_data2_fixformat.write.saveAsTable("ad_data2_fixformat")