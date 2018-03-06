from pyspark import SparkConf, SparkContext

# conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
# sc = SparkContext(conf = conf)

sc = SparkContext("yarn")

def parse_order(order):
	fields = order.split(",")
	return ((int(fields[0])), float(fields[2]))

orders = sc.textFile("hdfs://.../customer-orders.csv")
rdd = orders.map(parse_order)
purchase_per_person = rdd.reduceByKey(lambda x, y: x + y)
purchases_sorted = purchase_per_person.sortBy(lambda x: x[1])
results = purchases_sorted.collect()
for result in results:
	print("customerID: {}, Total Spent: ${:.2f}".format(result[0], result[1]))