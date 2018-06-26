from pyspark import SparkContext

# add a function to find the min and max simultaneously
def min_max(v1, v2):
	outmin = v2[0] if v1[0] > v2[0] else v1[0]
	outmax = v1[1] if v1[1] > v2[1] else v2[1]
	return (outmin, outmax)

sc = SparkContext(appName="avgTemps")
lines = sc.textFile("/user/x_pauth/data/temperature-readings.csv").cache()
lines = lines.map(lambda a: a.split(';'))
# filter by the years
lines = lines.filter(lambda x: int(x[1][0:4]) >= 1960 and int(x[1][0:4]) <= 2014)

# create tuple mapping for the function min_max()
temperatures = lines.map(lambda x: (x[0]+';'+x[1], (float(x[3]), float(x[3]))))
# apply min_max() on the data
temperatures = temperatures.reduceByKey(min_max)

# sum up the values for each month
monthStationTemps = temperatures.map(lambda x: (x[0][:-3], (sum(x[1]), 2)))

# calculate the average 
avgMonthStationTemps = monthStationTemps.reduceByKey(lambda v1, v2: (v1[0]+v2[0], v1[1]+v2[1]))
avgMonthStationTemps = avgMonthStationTemps.map(lambda x: (x[0], x[1][0]/x[1][1]))

# save the results
avgMonthStationTemps.saveAsTextFile("/user/x_pauth/results/avgTempsByMonthAndStation")
