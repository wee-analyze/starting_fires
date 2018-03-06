# there are 2 weather stations in the dataset and observations for everyday in a certain year.

from pyspark import SparkConf, SparkContext

conf = SparkConf()#.setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("/sparkcourse/1800.xls")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])  #keeping only TMIN labels
stationTemps = minTemps.map(lambda x: (x[0], x[2]))  #making new structure; only station and tmp. Also, become a key:value pair
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
results = minTemps.collect();

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1])) #tab character and trancting to 2 decimal places
