from pyspark import SparkConf, SparkContext

# conf = SparkConf().setMaster("local[*]").setAppName("WordCount")
# sc = SparkContext(conf = conf)

sc = SparkContext("yarn")

input = sc.textFile("hdfs://.../Book.txt")
words = input.flatMap(lambda x: x.split())  #splits each row (one paragraph in a row) to each word as row
wordCounts = words.countByValue() #quick way to get count of unique values

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))


#python trick
#takes care of format issues so it diplays in our terminal in case something was
#encoded at utf8 or unitext. Converts from unicode to ascii format and ignore any conversion errors
#cleanWord = word.encode('ascii', 'ignore')  