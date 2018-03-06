# using a broadcast table to transmit table of ID and Names to all nodes and keep it there for when
# the nodes need to use it instead of sending it multiple times.

from pyspark import SparkConf, SparkContext

#making dictionary of ids:movie names

def parse_names(line):
	movie_names = {}
	fields = line.split("|")
	return (int(fields[0]), fields[1].encode("utf8"))

#conf = SparkConf()#.setMaster("local").setAppName("PopularMovies")
sc = SparkContext("yarn")

id_lines = sc.textFile("hdfs://...ml100k/u.item")
id_lines_rdd = id_lines.map(parse_names)
names_dict = id_lines_rdd.collectAsMap()   # creates key:value dict (id:movie)


# Sends our mapping dictionary we made, one time, to every node in cluster and keeps it there
# so it's available when needed and all nodes will know it as the object names_dict
nameDict = sc.broadcast(names_dict)

lines = sc.textFile("hdfs://..ml100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map(lambda x : (x[1], x[0]))
sortedMovies = flipped.sortByKey()

sortedMoviesWithNames = sortedMovies.map(lambda countMovie : (nameDict.value[countMovie[1]], countMovie[0]))

results = sortedMoviesWithNames.collect()

for result in results:
    print (result)
