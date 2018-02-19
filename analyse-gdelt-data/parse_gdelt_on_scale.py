#coding: utf-8
#
from gdelt_data_parser import gdelt_data_parser as gdp
import re
import subprocess



from pyspark import SparkConf, SparkContext
conf = SparkConf()
sc = SparkContext(conf = conf)

#read all export.CSV data into a single RDD
directory = "/user/jdoering/gdelt/gkg/"
#get file list
cat = subprocess.Popen(["hadoop", "fs", "-stat", "%n", directory + "*"], stdout=subprocess.PIPE)
files = []
for line in cat.stdout:
    if (line.find('.gkg.csv') != -1):
        files.append(directory + line.replace('\n',''))

#read all export.CSV data into a single RDD
print(len(files))
lines = sc.parallelize(files)
parsed_data = lines.map(gdp.cust_parse_gkg_data).collect()

#for l in parsed_data[1:10]:
#    print(l)
