#coding: utf-8
#
from gdelt_data_parser import gdelt_data_parser as gdp
import re
import subprocess


#read all export.CSV data into a single RDD
directory = "/user/jdoering/gdelt/gkg/"
#get file list
cat = subprocess.Popen(["hadoop", "fs", "-stat", "%n", directory + "*"], stdout=subprocess.PIPE)
files = []
for line in cat.stdout:
    if (line.find('.gkg.csv') != -1):
        files.append(directory + line.replace('\n',''))


parsed_data = sc.parallelize(files[:10], 6000).map(gdp.cust_parse_gkg_data_csv).collect()


for l in parsed_data[:10]:
    print(l)

#lines = sc.parallelize(files[100], 10)
#parsed_data = lines.map(gdp.cust_parse_gkg_data).collect()

