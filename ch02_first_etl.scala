val ad_data1=sqlContext.read.table("ad_data1")
ad_data1.persist()
val ad_data1_us=ad_data1.filter("country='USA'")
ad_data1.count()
ad_data1_us.count
ad_data1_us.take(5)
//val ad_data1_reordered = ad_data1_us.select("campaign_id", "date", "time", "keyword", "display_site", "placement","was_clicked", "cpc")
val ad_data1_converted = ad_data1_us.selectExpr("campaign_id","date","time","upper(trim(keyword)) as keyword", "display_site", "placement","was_clicked", "cpc")
ad_data1_converted.count
ad_data1_converted.write.saveAsTable("ad_data1_converted")
