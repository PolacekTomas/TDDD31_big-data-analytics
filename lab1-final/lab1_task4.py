from pyspark import SparkContext

sc = SparkContext(appName="avgTemps")
temperatures = sc.textFile("/user/x_pauth/data/temperature-readings.csv").cache()
temperatures = temperatures.map(lambda a: a.split(';'))
temperatures = temperatures.map(lambda x: (x[0], float(x[3])))
# find max temperature for each station
temperatures = temperatures.reduceByKey(max)
# filter with regard to the max temperature
temperatures = temperatures.filter(lambda x: x[1] >= 25 and x[1] <= 30)

# read the precipitation data 
precipitation = sc.textFile("/user/x_pauth/data/precipitation-readings.csv").cache()
precipitation = precipitation.map(lambda a: a.split(';'))
# calculate the daily precipitation
precipitation = precipitation.map(lambda x: (x[0]+';'+x[1], float(x[3])))
precipitation = precipitation.reduceByKey(lambda v1,v2: v1+v2)
precipitation = precipitation.map(lambda x: (x[0].split(';')[0], x[1]))
# find the max daily precipitation for each station
precipitation = precipitation.reduceByKey(max)
# filter with regard to the max daily precipitation
precipitation = precipitation.filter(lambda x: x[1] >= 100 and x[1] <= 200)

# join the RDDs
stations = temperatures.join(precipitation)
# save the results
stations.saveAsTextFile("/user/x_pauth/results/stationsTemperaturesPrecipitation")



