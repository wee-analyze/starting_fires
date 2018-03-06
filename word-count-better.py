import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower()) #breaks up words and puncuations

# conf = SparkConf().setMaster("local").setAppName("WordCount")
# sc = SparkContext(conf = conf)

sc = SparkContext("yarn")

input = sc.textFile("hdfs://.../Book.txt")
words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
