#finds most popular movies in a movie database of 100k, sorts it and prints it out
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

hdfs_dir = 'hdfs:/user/jdoering/data'

input = sc.textFile(hdfs_dir + "/ml-100k/u.data")

#sum of all ratings per movie
ratings = input.map(lambda x: (x.split()[1], (int(x.split()[2]), 1))).reduceByKey( lambda x,y: (x[0]+y[0], x[1]+y[1]))
ratings = ratings.map(lambda x: (x[0], float(x[1][0])/x[1][1])).map(lambda x: (x[1], x[0])).sortByKey()

#sorted movies watched
viewings = input.map(lambda x: (x.split() [1], 1)).reduceByKey(lambda x,y,: x+y)
viewings = viewings.map(lambda x: (x[1], x[0])).sortByKey()
viewings = viewings.map(lambda x: (x[1], x[0]))


results = viewings.collect()
#results = ratings.collect()

for r in results:
    print '{}: {}'.format(str(r[0]), r[1])
