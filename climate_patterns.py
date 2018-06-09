from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys, operator
import re
from pyspark.mllib.clustering import KMeans, KMeansModel
import numpy as np
from math import sqrt

reload(sys)
sys.setdefaultencoding('utf8')

conf = SparkConf().setAppName('stations location')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

TMAX_input = "/user/liufanl/TMAX"
TMIN_input = "/user/liufanl/TMIN"

TMAX_data = sc.textFile(TMAX_input)
TMIN_data = sc.textFile(TMIN_input)

def stations_data_delimiter(text):
    begin = 10
    end = 15
    newvalue = []
    for i in xrange(365):
        value = text[begin+i*8:end+i*8]
        newvalue.append(value)
    return newvalue

def replace_missing_value(list):
    #newlist = []
    for i in xrange(len(list)):
        if int(list[i]) == -9999:
            list[i] = int(list[i-1])
        else:
            list[i] = int(list[i])
    list_mean = sum(list) / float(len(list))
    newlist = [value - list_mean for value in list]
    return newlist

def many_missing_value(list):
    missing_count = 0
    for i in xrange(len(list)):
        if int(list[i]) == -9999:
            missing_count = missing_count + 1
    if missing_count >= 36:
        return True
    else:
        return False


TMAX_data_split = TMAX_data.map(lambda row: (row[0:4], stations_data_delimiter(row)))\
    .filter(lambda (year, list): many_missing_value(list) == False)\
    .map(lambda (year, list): (year, replace_missing_value(list)))

TMIN_data_split = TMIN_data.map(lambda row: (row[0:4], stations_data_delimiter(row)))\
    .filter(lambda (year, list): many_missing_value(list) == False)\
    .map(lambda (year, list): (year, replace_missing_value(list)))

TMAX_data_list = TMAX_data_split.map(lambda (year, list): list)
TMIN_data_list = TMIN_data_split.map(lambda (year, list): list)

# Build the model (cluster the data)
# kmeans model based on whole year's tempature, k =
kList = [100, 200, 300, 400, 500]
for k in kList:
    TMAX_model = KMeans.train(TMAX_data_list, k, maxIterations=10, initializationMode="random")
    TMIN_model = KMeans.train(TMIN_data_list, k, maxIterations=10, initializationMode="random")

    def TMAX_error(point):
        center = TMAX_model.centers[TMAX_model.predict(point)]
        return sqrt(sum([x**2 for x in (point - center)]))

    def TMIN_error(point):
        center = TMIN_model.centers[TMIN_model.predict(point)]
        return sqrt(sum([x**2 for x in (point - center)]))

    TMAX_WSSSE = TMAX_data_list.map(lambda point: TMAX_error(point)).reduce(lambda x, y: x + y)
    print("TMAX Within Set Sum of Squared Error = " + str(TMAX_WSSSE))
    print "******* (^_^) *******"

    TMIN_WSSSE = TMIN_data_list.map(lambda point: TMIN_error(point)).reduce(lambda x, y: x + y)
    print("TMIN Within Set Sum of Squared Error = " + str(TMIN_WSSSE))
    print "******* (^_^) *******"


TMAX_clusters = TMAX_data_list.map(lambda row: TMAX_model.predict(row))
TMIN_clusters = TMIN_data_list.map(lambda row: TMIN_model.predict(row))

TMAX_cluster_zip = TMAX_data_split.zip(TMAX_clusters)
TMAX_cluster_df = TMAX_cluster_zip.map(lambda ((year, list), cluster): (year, list, int(cluster)))\
    .toDF(["year", "list", "cluster"])

TMIN_cluster_zip = TMIN_data_split.zip(TMIN_clusters)
TMIN_cluster_df = TMIN_cluster_zip.map(lambda ((year, list), cluster): (year, list, int(cluster)))\
    .toDF(["year", "list", "cluster"])

#stations_unique_cluster = stations_cluster_df.dropDuplicates(["cluster"])

print TMAX_cluster_df.show()
print "******* (^_^) *******"

print TMIN_cluster_df.show()
print "******* (^_^) *******"
'''
stations_unique_cluster_rdd = stations_unique_cluster.rdd.coalesce(2)
stations_unique_cluster_rdd.saveAsTextFile("station_kmeans_result")

print TMAX_data_split.take(1)
print "******* (^_^) *******"
'''


