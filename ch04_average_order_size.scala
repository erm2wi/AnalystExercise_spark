val orders=sqlContext.read.table("orders")
orders.persist
val order_details=sqlContext.read.table("order_details")
order_details.persist
val orders_may=orders.filter($"order_dtm" rlike "^2013-05-.*$")
orders_may.show(5)
val tablet_order_details=order_details.filter("prod_id=1274348")
tablet_order_details.show(5)
//will result in two same columns
//val order_size=orders_may.join(tablet_order_details,orders_may("order_id")===tablet_order_details("order_id"),"inner")
//order_size.show(5)
//val order_with_product=order_size.select("order_id")
val order_size=orders_may.join(tablet_order_details,"order_id")
order_size.printSchema
val prod_order=order_size.select("order_id")
prod_order.count
orders.count
val prod_order_details=prod_order.join(order_details,"order_id")
prod_order_details.groupBy("order_id").count.show(5)
val order_size=prod_order_details.groupBy("order_id").count
order_size.describe("count").show()
val avg_order_size=order_size.agg(avg("count"))
avg_order_size.show()
