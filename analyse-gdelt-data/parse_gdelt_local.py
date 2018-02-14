#parses
from gdelt_data_parser import gdelt_data_parser as gdp

from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("Parse GKG")
sc = SparkContext(conf = conf)

#read all export.CSV data into a single RDD
lines = sc.textFile("./data/*.gkg.csv")
export_data = lines.map(gdp.parse_gkg_data)

for l in export_data.collect():
    #print(l)
    pass