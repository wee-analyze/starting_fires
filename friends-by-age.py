# put fakefriends file in HDFS directory called /sparkcourse

from pyspark import SparkConf, SparkContext

# conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
#sc = SparkContext(conf = conf)  # didn't need if doing line by line

sc = SparkContext("yarn")

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("hdfs://../fakefriends.xls")
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
for result in results:
    print(result)

#totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
#mapValues(lambda x: (x,1))
#looks only at value and returns a tupel with the value again and 1 because we are trying to make a count
#original (44,3)
#after mapValues(44,(3,1))

#reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
#matches keys and adds values together
#person 1: (44,(3,1))
#person 2: (44,(19,1))
#after reduceByKey: (44,(22,2))

#averageByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
#finds average number of friends for that age using the tuple