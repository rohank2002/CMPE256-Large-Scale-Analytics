#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function
from __future__ import print_function
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import pandas as pd
import sys
from pyspark import SparkConf, SparkContext
conf = SparkConf()
sc = SparkContext(conf=conf)
import sys
if sys.version >= '3':
    long = int
import math
from pyspark.sql import SparkSession
import numpy as np
# $example on$
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import lit
# $example off$

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("ALSExample")\
        .getOrCreate()
    # $example on$
    
    train = sc.textFile(sys.argv[1])
    ratingsRDD = train.map(lambda l: l.split('\t'))\
        .map(lambda l: Row(user=int(l[0]), product=int(l[1]), rating=float(l[2])))
    ratings = spark.createDataFrame(ratingsRDD)
    #(training, test) = ratings.randomSplit([0.8, 0.2])
    test=sc.textFile(sys.argv[2])
    testdata = test.map(lambda l: l.split('\t'))\
	.map(lambda l: Row(user=int(l[0]), product=int(l[1])))
    testrdd = spark.createDataFrame(testdata)
    # Build the recommendation model using ALS on the training data
    # Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    s2 = ratings.rdd.map(lambda w:w[1])
    #ratings.rdd.repartition(1).saveAsTextFile("rohan")
    count = s2.count()
    sum = s2.sum()
    avg = sum/count
    
    als = ALS(maxIter=3, regParam=0.5, userCol="user", itemCol="product", ratingCol="rating",
              coldStartStrategy="nan")
    model = als.fit(ratings)
    
    # Evaluate the model by computing the RMSE on the test data
    predictions = model.transform(testrdd).rdd.map(lambda w: w[2])
    #predictions.repartition(1).saveAsTextFile("op")
    c=predictions.collect()
    op=[]
    for item in c:
    	if item>0 or item<0:
        	op.append(item)
    	else:
        	op.append(avg)
            
        
    #print(c)
    f=open('op.txt','w+')
    
    for item in op:
    	f.write("%f\r\n"%item)
    f.close()
#print(c)
    #predictions.to_csv(r'op.txt', header=None, index=None, sep=' ', mode='a')
    #predictions.write.format("csv").save("op.txt")
    
    #predictions.repartition(1).saveAsTextFile("tmp5")
    #c=predictions[0].tolist()
    	
    #c=predictions[2].tolist()
    #a=list(c)	
    # predictions.repartition(1).saveAsTextFile("op")
    #np.savetxt(r'o10101.txt', predictions, fmt='%d')
    #evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                  #  predictionCol="prediction")
    #rmse = evaluator.evaluate(predictions)
    #print("Root-mean-square error = " + str(rmse))

    # Generate top 10 movie recommendations for each user
    #userRecs = model.recommendForAllUsers(10)
    # Generate top 10 user recommendations for each movie
   # movieRecs = model.recommendForAllItems(10)

    # Generate top 10 movie recommendations for a specified set of users
   # users = ratings.select(als.getUserCol()).distinct().limit(3)
    #userSubsetRecs = model.recommendForUserSubset(users, 10)
    # Generate top 10 user recommendations for a specified set of movies
   # movies = ratings.select(als.getItemCol()).distinct().limit(3)
   # movieSubSetRecs = model.recommendForItemSubset(movies, 10)
    # $example off$
   # userRecs.show()
   # movieRecs.show()
   # userSubsetRecs.show()
   # movieSubSetRecs.show()

    spark.stop()
