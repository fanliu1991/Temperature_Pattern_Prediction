from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys, operator
import re
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt

reload(sys)
sys.setdefaultencoding('utf8')

conf = SparkConf().setAppName('location climate match')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

stations_input = "/Users/liufanl/Documents/CMPT733/Project/stations2500.txt"
climate_input = "/Users/liufanl/Documents/CMPT733/Project/location_climate.txt"

stations_data = sc.textFile(stations_input)
climate_data = sc.textFile(climate_input)


def integer_extract(value):
    number = float(value)
    integer = int(number)
    return integer

def digit_extract(value):
    number = float(value)
    integer = int(number)
    decimal = float(number) - integer
    return decimal

def digit_modification(value):
    number = float(value)
    integer = int(number)
    decimal = float(number) - integer
    if decimal>=0 and decimal<0.5:
        new_decimal = 0.25
    elif decimal>=0.5 and decimal<1:
        new_decimal = 0.75
    elif decimal<0 and decimal>-0.5:
        new_decimal = -0.25
    else:
        new_decimal = -0.75
    new_number = integer + new_decimal
    return new_number


stations_data_df = stations_data.map(lambda row: row.split(","))\
    .map(lambda (ID, latitude, longitude, elevation, cluster): (ID, float(latitude), float(longitude), float(elevation), int(cluster),
                                                                digit_modification(latitude), digit_modification(longitude)))\
    .toDF(["ID", "latitude", "longitude", "elevation", "cluster", "mod_latitude", "mod_longitude"])

climate_data_df = climate_data.map(lambda row: row.split(","))\
    .map(lambda (longitude, latitude, climate_class): (float(latitude), float(longitude), str(climate_class)))\
    .toDF(["latitude", "longitude", "climate_class"])

'''
print stations_data_df.show(truncate=False)
print "******* (^_^) *******"

print climate_data_df.show(truncate=False)
print "******* (^_^) *******"
'''

join_condition = [stations_data_df["mod_latitude"] == climate_data_df["latitude"], stations_data_df["mod_longitude"] == climate_data_df["longitude"]]
join_df = stations_data_df.join(climate_data_df, join_condition, "inner")

print join_df.show(truncate=False)
print "******* (^_^) *******"

print join_df.count() #2315
print "******* (^_^) *******"