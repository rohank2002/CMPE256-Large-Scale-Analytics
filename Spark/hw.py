import re
import sys
from pyspark import SparkConf, SparkContext
conf = SparkConf()
sc = SparkContext(conf=conf)
lines = sc.textFile(sys.argv[1])
words = lines.flatMap(lambda l: re.split(r'[^\w]+', l))
pairs = words.map(lambda w: (len(w), 1))

counts = pairs.reduceByKey(lambda n1, n2: n1 + n2).sortByKey().values()
counts.repartition(1).saveAsTextFile(sys.argv[2])
sc.stop()

















