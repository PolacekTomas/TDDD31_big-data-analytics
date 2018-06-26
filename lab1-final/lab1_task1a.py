from pyspark import SparkContext

sc = SparkContext(appName="maxMin")
lines = sc.textFile("data/temperature-readings.csv").cache()
#lines = sc.textFile("/user/x_pauth/data/temperature-readings.csv").cache()
lines = lines.map(lambda a: a.split(';'))
# filter for the years
lines = lines.filter(lambda x: int(x[1][0:4]) >= 1950 and int(x[1][0:4]) <= 2014)

# extract year, temperature and station
temperatures = lines.map(lambda x: (x[1][0:4], (float(x[3]), x[0])))

# find min and max
maxTemperatures = temperatures.reduceByKey(max)
minTemperatures = temperatures.reduceByKey(min)
# sort by temperature
maxTemperaturesSorted = maxTemperatures.sortBy(ascending=False, keyfunc=lambda k: k[1][0])
minTemperaturesSorted = minTemperatures.sortBy(ascending=False, keyfunc=lambda k: k[1][0])
# save results into file
maxTemperaturesSorted.saveAsTextFile("results/resultsMax")
minTemperaturesSorted.saveAsTextFile("results/resultsMin")
#maxTemperaturesSorted.saveAsTextFile("/user/x_pauth/data/resultsMax")
#minTemperaturesSorted.saveAsTextFile("/user/x_pauth/data/resultsMin")
