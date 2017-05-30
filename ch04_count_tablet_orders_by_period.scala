sqlContext.sql("""create external table order_details(
order_id int,
prod_id int)
row format delimited fields terminated by '\t'
location '/dualcore/order_details/'""")



val order_details=sqlContext.read.table("order_details")
order_details.persist()

order_details.show(5)
val tablet_order_details=order_details.filter("prod_id=1274348")
tablet_order_details.count
//not working ==order_details.filter("prod_id==1274348")
//not working <==>order_details.filter("prod_id<==>1274348")
order_details.filter("prod_id!=1274348")  //works
order_details.filter("prod_id<>1274348")  //works
//not work order_details.filter($"prod_id"=1274348)
order_details.filter($"prod_id"===1274348)
order_details.filter($"prod_id"!==1274348)
val orders=sqlContext.read.table("orders")
orders.persist()

//not work orders.filter("order_dtm like '^2013-0[2-5]-\\d{2}\\s.*$'")
val recent_orders=orders.filter($"order_dtm" rlike "^2013-0[2-5]-\\d{2}\\s.*$")
recent_orders.count
recent_orders.show(5)
//not work, as order_id exits in both table
//recent_orders.join(tablet_order_details,$"order_id"===$"order_id","inner")
val recent_order_details=recent_orders.join(tablet_order_details,recent_orders("order_id")===tablet_order_details("order_id"),"inner")
import org.apache.spark.sql.functions._
val recent_order_details_yrmth=recent_order_details.withColumn("Yrmth",substring($"order_dtm",0,7))
recent_order_details_yrmth.show(5)
val order_by_mth=recent_order_details_yrmth.groupBy("yrmth").count.orderBy($"yrmth".asc)
order_by_mth.show(5)
