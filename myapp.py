import sys
from pyspark import SparkConf, SparkContext
conf = SparkConf()
sc = SparkContext(conf=conf)
print ("%d lines" % sc.textFile('file://'+sys.argv[1]).count())