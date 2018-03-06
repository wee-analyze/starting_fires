from pyspark import SparkConf, SparkContext

# conf = SparkConf().setMaster("local").setAppName("PopularHero")
# sc = SparkContext(conf = conf)

sc = SparkContext("yarn")

def countCoOccurences(line):
    elements = line.split()
    return (int(elements[0]), len(elements) - 1)

def parseNames(line):
    fields = line.split('\"')  #deliminates on the ". 1 "peter parker" will end up being 3 fields on the split. 3rd field is empty.
    return (int(fields[0]), fields[1].encode("utf8"))

names = sc.textFile("hdfs://../Marvel-Names.txt")
namesRdd = names.map(parseNames)

lines = sc.textFile("hdfs://../Marvel-Graph.txt")

pairings = lines.map(countCoOccurences) #(id, number of characters been with)
totalFriendsByCharacter = pairings.reduceByKey(lambda x, y : x + y)
flipped = totalFriendsByCharacter.map(lambda xy : (xy[1], xy[0]))

mostPopular = flipped.max()

mostPopularName = namesRdd.lookup(mostPopular[1])[0]  #lookup returns a list of all the VALUES of the key.
#since we only have one value in the list that was returned in the lookup then doing[0] returned that value

print(str(mostPopularName) + " is the most popular superhero, with " + \
    str(mostPopular[0]) + " co-appearances.")
