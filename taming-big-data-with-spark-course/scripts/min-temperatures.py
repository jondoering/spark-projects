#finds minimum temperatur in file per station and prints it out

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3])
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("file:///home/jonathan/PycharmProjects/SparkUdemi/1800.csv")
parsedLines = lines.map(parseLine)

minTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2],))
minTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))
#num_of_entries = stationTemps.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))
results = minTemps.collect();
#num_of_entries = stationTemps
#results = num_of_entries.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))