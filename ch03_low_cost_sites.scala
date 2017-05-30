val ad_data1_converted=sqlContext.read.table("ad_data1_converted")
val ad_data2_fixformat=sqlContext.read.table("ad_data2_fixformat")
val ad_data = ad_data1_converted unionAll ad_data2_fixformat
ad_data.count
ad_data.show(5)
val ad_data_clicked = ad_data.filter("was_clicked=1")
ad_data_clicked.show(5)
val ad_data_bysite=ad_data_clicked.groupBy("display_site")
val ad_data_cpc_by_site=ad_data_bysite.sum("cpc")
ad_data_cpc_by_site.count
ad_data_cpc_by_site.show(109)
ad_data_cpc_by_site.printSchema()
val cpc_sorted=ad_data_cpc_by_site.sort(asc("sum(cpc)"))  //asc or desc
cpc_sorted.take(5)
