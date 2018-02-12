#Simple word count in a text file
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:////home/jonathan/PycharmProjects/SparkUdemi/Book.txt")
words = input.flatMap(lambda x: x.split(' '))
words2 = words.map(lambda x: re.compile(r'[^A-Za-z0-9]').split(x)[1])

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

wordCounts = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()

results = wordCounts.collect();

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
