val ad_data1_converted=sqlContext.read.table("ad_data1_converted")
val ad_data2_fixformat=sqlContext.read.table("ad_data2_fixformat")
val ad_data = ad_data1_converted unionAll ad_data2_fixformat
ad_data.persist
ad_data.count
ad_data.show(5)
val ad_data_clicked = ad_data.filter("was_clicked=1")
ad_data_clicked.count
