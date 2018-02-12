#finds most popular movies in a movie database and prints it out
#using a broadcast variable as lookup

data_dir = 'file:///home/jdoering/spark/PySpark_Exercises/data/'
hdfs_dir = 'hdfs://user/jdoering/data/')

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

##Broadcast Variables: Spark transfers information once and keep it there (e.g. static lookup tables)
#lo

with open(data_dir + "ml-100k/u.item", 'r') as file:
    movie_names = {}
    for l in file.readlines():
        fields = l.split('|');
        movie_names[fields[0]] = fields[1];

mov_names_bc = sc.broadcast(movie_names)

def get_movie_name(id):
    return mov_names_bc.value[id]


input = sc.textFile(data_dir + "ml-100k/u.data")

#calculate average rating and viewings
ratings = input.map(lambda x: (x.split()[1], (int(x.split()[2]), 1))).reduceByKey( lambda x,y: (x[0]+y[0], x[1]+y[1]))
ratings = ratings.map(lambda x: (x[0], float(x[1][0])/x[1][1], x[1][1])).filter(lambda x: x[2] > 50).map(lambda x: (x[1], (get_movie_name(x[0]), x[2]))).sortByKey()

#calculate movies watched
viewings = input.map(lambda x: (x.split() [1], 1)).reduceByKey(lambda x,y,: x+y)
viewings = viewings.map(lambda x: (x[1], x[0])).sortByKey()
viewings = viewings.map(lambda x: (x[1], x[0]))



##Execute
#results = viewings.collect()
results = ratings.collect()

for r in results:
    print '{}: rating: {}, vieiwings: {}'.format(str(r[1][0]), r[0], r[1][1])
