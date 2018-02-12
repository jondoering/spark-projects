#Breadth-first-search: iterrative algorithm example for degres of separation
#how many hops do you need to find connections

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Assignment one")
sc = SparkContext(conf = conf)


input = sc.textFile("file:////Users/Jonathan/Developing/Python/SparkUdemi/assignment1/customer-orders.csv")


input = input.map(lambda x: (int(x.split(',')[0]), float(x.split(',')[2]))).reduceByKey(lambda x,y: x+y)
input = input.map(lambda x: (x[1], x[0])).sortByKey().map(lambda x: (x[1], x[0]))
result = input.collect()

for r in result:
    print r