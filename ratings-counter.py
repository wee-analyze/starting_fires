from pyspark import SparkConf, SparkContext # need to import these
import collections


# configuring whether we are using a cluster or just one computer ("local[*]"). 
# conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
# sc = SparkContext(conf = conf)

sc = SparkContext("yarn")

lines = sc.textFile("hdfs://...ml100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
