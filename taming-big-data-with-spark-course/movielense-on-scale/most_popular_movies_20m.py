#finds most popular movies in a movie database of 100k, sorts it and prints it out
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

#cur_dir = 'hdfs:/user/jdoering/data'

cur_dir = 'file:///home/pentaho/Projects/PycharmProjects/PySpark_Exercises/data'
input = sc.textFile(cur_dir + "/ml-20m/ratings_sub.csv")

#filter header
header = input.first()
data = input.filter(lambda x: x != header)

#average of all ratings per movie
ratings = data.map(lambda x: (x.split(',')[1], (float(x.split(',')[2]), 1))).reduceByKey( lambda x,y: (x[0]+y[0], x[1]+y[1]))
ratings = ratings.map(lambda x: (x[0], (float(x[1][0])/x[1][1], x[1][1])))
ratings = ratings.map(lambda x: (x[1][0], (x[0], x[1][1]))).sortByKey(ascending = False)

#sorted movies watched
viewings = input.map(lambda x: (x.split(',')[1], 1))
viewings = viewings.reduceByKey(lambda x,y,: x+y)
viewings = viewings.map(lambda x: (x[1], x[0])).sortByKey()
#viewings = viewings.map(lambda x: (x[1], x[0]))


#results = viewings.collect()
results = ratings.collect()


for i in range(0, 10):
    #print(r)
    r = results[i]
    print('{}: {} in {}'.format(str(r[1][0]), r[0], r[1][1]))
