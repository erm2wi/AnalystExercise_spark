create external table orders(
order_id int,
cust_id int,
order_dtm string)
row format delimited fields terminated by '\t'
location '/dualcore/orders'