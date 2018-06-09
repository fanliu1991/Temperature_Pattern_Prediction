from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys, operator
import re
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt

reload(sys)
sys.setdefaultencoding('utf8')

conf = SparkConf().setAppName('stations location')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

stations_input = "/Users/liufanl/Documents/CMPT733/Project/sample_stations.txt"

stations_data = sc.textFile(stations_input)

def stations_data_delimiter(text):
    newtext = text[:11] + "," + text[12:20] + "," + text[21:30] + "," + text[31:37]
    return newtext

station_data_split = stations_data.map(lambda row: stations_data_delimiter(row))\
    .map(lambda row: row.split(","))\
    .map(lambda (ID, latitude, longitude, elevation): (ID, float(latitude), float(longitude), float(elevation)))



station_data_list = station_data_split.map(lambda (ID, latitude, longitude, elevation): [latitude, longitude, elevation])


# Build the model (cluster the data)
# kmeans model based on latitude, longitude and elevation, k = 2500
model = KMeans.train(station_data_list, 2500, maxIterations=10, initializationMode="random")
def error(point):
    center = model.centers[model.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = station_data_list.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))
print "******* (^_^) *******"

clusters = station_data_list.map(lambda row: model.predict(row))

stations_cluster_zip = station_data_split.zip(clusters)
stations_cluster_df = stations_cluster_zip.map(lambda ((ID, latitude, longitude, elevation), cluster): (ID, latitude, longitude, elevation, int(cluster)))\
    .toDF(["ID", "latitude", "longitude", "elevation", "cluster"])

stations_unique_cluster = stations_cluster_df.dropDuplicates(["cluster"])

print stations_unique_cluster.show()
print "******* (^_^) *******"

stations_unique_cluster_rdd = stations_unique_cluster.rdd.coalesce(2)
stations_unique_cluster_rdd.saveAsTextFile("station_kmeans_result")

