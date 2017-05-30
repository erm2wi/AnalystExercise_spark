val orders=sqlContext.read.table("orders")
val orders_withYearMonth=orders.withColumn("year",regexp_extract($"order_dtm","^(2013)-(0[2345])-\\d{2}\\s.*$",1)).withColumn("month",regexp_extract($"order_dtm","^(2013)-(0[2345])-\\d{2}\\s.*$",2))
orders_withYearMonth.show(5)
orders_withYearMonth.filter("year=2013").show(5)
orders_withYearMonth.filter("year=2013").count
orders_withYearMonth.filter("year=2013").groupBy("month").count().show(15)