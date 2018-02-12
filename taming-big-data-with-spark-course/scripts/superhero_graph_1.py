#calculate super hero with most connections
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Super Hero Graph")
sc = SparkContext(conf = conf)

##Broadcast Variables: Spark transfers information once and keep it there (e.g. static lookup tables)
with open("/Users/Jonathan/Developing/Python/SparkUdemi/Marvel-Names.txt", 'r') as file:
    sh_names = {}
    for l in file.readlines():
        fields = l.split('"');
        sh_names[fields[0].replace(' ','')] = fields[1].replace('"','').replace('\n', '');

hero_names_bc = sc.broadcast(sh_names)

def get_hero_name(id):
    return hero_names_bc.value[id]


input = sc.textFile("file:////Users/Jonathan/Developing/Python/SparkUdemi/Marvel-Graph.txt")


#calculate super hero with most connections
heros_con = input.map(lambda x: (get_hero_name(x.split()[0]), len(x.split()))).reduceByKey(lambda x,y: x+y)
heros_con = heros_con.map(lambda x: (x[1], x[0])).sortByKey().map(lambda x: (x[1], x[0])) #sort by apperance
result = heros_con.collect();

with open('output.txt', 'w') as file:
    for r in result:
        file.write('{}: {}\n'.format(r[0], r[1]));