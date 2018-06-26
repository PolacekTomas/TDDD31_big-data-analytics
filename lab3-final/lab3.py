from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp 
from datetime import datetime
from pyspark import SparkContext
import pyspark

sc = SparkContext(appName="lab_kernel") 
sc.setLogLevel("ERROR")
def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2]) 
    # haversine formula
    dlon = lon2 - lon1 
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2 
    c = 2 * asin(sqrt(a))
    km = 6367 * c 
    return km

def time_to_seconds(time):
    return sum(x * int(t) for x, t in zip([3600, 60, 1], time.split(":")))

def date_to_days(a, b):
    return (datetime.strptime(a, "%Y-%m-%d") - datetime.strptime(b, "%Y-%m-%d")).days

def calc_weight(distance, days, time):
    dist_weight = exp(-(distance/h_distance)**2)
    date_weight = exp(-(days/h_date)**2)
    time_weight = exp(-(time/h_time)**2)
    return dist_weight + date_weight + time_weight
#    return dist_weight * date_weight * time_weight

h_distance = 100 # 100 km
h_date  = 3 # days 
h_time = 3*3600 # 3 hours
a = 58.4104 # Ryd's Volleyball court
b = 15.563 # Ryd's Volleyball court
date = "2013-06-04" # Some date

stations = sc.textFile("data/stations.csv").map(lambda x: x.split(';'))
# create a dict with station number and distance
stations = stations.map(
    lambda x: (x[0], haversine(float(x[3]), float(x[4]), a, b))).collectAsMap()

temps = sc.textFile("data/temperature-readings.csv")
temps = temps.map(lambda x: x.split(';'))   
temps = temps.filter(lambda x: x[1] < date)
temps = temps.map(lambda x: (
                                stations[x[0]], 
                                date_to_days(x[1], date), 
                                time_to_seconds(x[2]), 
                                float(x[3])
                            )).persist(pyspark.StorageLevel.MEMORY_AND_DISK)

for time in range(4,26,2):
    time = str(time).zfill(2)+':00:00'
    secs = time_to_seconds(time)
    forecast = temps.map(lambda x: (x[3], calc_weight(x[0], x[1], secs-x[2])))
    forecast = forecast.map(lambda x: (x[0]*x[1], x[1]))
    forecast = forecast.reduce(lambda x,y: (x[0]+y[0], x[1]+y[1]))
    forecast = forecast[0]/forecast[1]
    print(time, forecast)