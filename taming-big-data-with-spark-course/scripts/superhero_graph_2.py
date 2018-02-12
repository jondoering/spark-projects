#Breadth-first-search: iterrative algorithm example for degres of separation
#how many hops do you need to find connections

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[*]").setAppName("DegreeeOfSeparation")
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

#####

startCharacterID = 5306
targetCharacterID = 14

hitCounter = sc.accumulator(0)

def convert_to_BFS(line):

    split = line.split(' ')[:-1]

    id = int(split[0])
    connections = []

    for k in split[1:]:
        #print split[1:]
        connections.append(int(k))

    color = 'WHITE'
    distance = 9999

    if (id == startCharacterID):
        color = 'GRAY'
        distance = 0

    return (id, (connections, distance, color))

def createStartingRDD():

    input = sc.textFile("file:////Users/Jonathan/Developing/Python/SparkUdemi/Marvel-Graph.txt")
    return input.map(convert_to_BFS)

def bfsMap(node):
#Creates new grey nodes based on an exitsin node

    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    if (color == 'GRAY'):

        for c in connections:
            newCharacterID = c
            newDistance = distance + 1
            newColor = 'GRAY'
            if (targetCharacterID == c):
                hitCounter.add(1)

            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)

        color = 'BLACK'

    results.append((characterID, (connections, distance, color)))
    return results

def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    dist1  = data1[1]
    dist2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = color1
    edges = []

    # See if one is the original node with its connections.
    # If so preserve them.
    if (len(edges1) > 0):
        edges.extend(edges1)
    if (len(edges2) > 0):
        edges.extend(edges2)

    # Preserve minimum distance
    if (dist1 < distance):
        distance = dist1

    if (dist2 < distance):
        distance = dist2

    # Preserve darkest color
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2

    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1

    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1

    return (edges, distance, color)



###MAIN Programme
iterationRdd = createStartingRDD()
for iter in range(0,10):

    print "Running BSF iteration nr. {}".format(iter+1)

    mapped = iterationRdd.flatMap(bfsMap)

    print "processing {} values".format(mapped.count())

    if (hitCounter.value > 0):
        print "hit the target character from {} different connection(s)".format(hitCounter.value)
        break

    iterationRdd = mapped.reduceByKey(bfsReduce)