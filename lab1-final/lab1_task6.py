from pyspark import SparkContext

# add a function to find the min and max simultaneously
def min_max(v1, v2):
	outmin = v2[0] if v1[0] > v2[0] else v1[0]
	outmax = v1[1] if v1[1] > v2[1] else v2[1]
	return (outmin, outmax)

sc = SparkContext(appName="avgTempOstg")

# read the list of stations in Ostergotland
stationsOstg = sc.textFile("/user/x_pauth/data/stations-Ostergotland.csv").cache()
stationsOstg = stationsOstg.map(lambda a: a.split(';')[0]).collect()

temperatures = sc.textFile("/user/x_pauth/data/temperature-readings.csv").cache()
temperatures = temperatures.map(lambda a: a.split(';'))
temperatures = temperatures.filter(lambda x: int(x[1][0:4]) >= 1950 and int(x[1][0:4]) <= 2014)
temperatures = temperatures.filter(lambda x: x[0] in stationsOstg)

# create tuple mapping for the function min_max()
temperatures = temperatures.map(lambda x: (x[0]+';'+x[1], (float(x[3]), float(x[3]))))
# apply min_max() on the data
temperatures = temperatures.reduceByKey(min_max)

# calculate the monthly average
temperatures = temperatures.map(lambda x: (x[0][:-3], (sum(x[1]), 2)))
temperatures = temperatures.reduceByKey(lambda v1, v2: (v1[0]+v2[0], v1[1]+v2[1]))
temperatures = temperatures.map(lambda x: (x[0], x[1][0]/x[1][1]))

# calculate the average over the stations in Ostergotland
temperatures = temperatures.map(lambda x: (x[0].split(';')[1], (x[1], 1)))
temperatures = temperatures.reduceByKey(lambda v1,v2: (v1[0]+v2[0], v1[1]+v2[1]))
temperatures = temperatures.map(lambda x: (x[0], x[1][0]/x[1][1]))

# calculate the long term average and collecting the result as a dict
tempAvg = temperatures.filter(lambda x: int(x[0][:4]) <= 1980)
tempAvg = tempAvg.map(lambda x: (x[0][-2:], (x[1], 1)))
tempAvg = tempAvg.reduceByKey(lambda v1,v2: (v1[0]+v2[0], v1[1]+v2[1]))
tempAvg = tempAvg.map(lambda x: (int(x[0]), x[1][0]/x[1][1]))
tempAvg = tempAvg.collectAsMap()

# calculate the difference to the long term average for each month
differences = temperatures.map(lambda x: (x[0], x[1]-tempAvg[int(x[0][-2:])]))

# save the results
differences.saveAsTextFile("/user/x_pauth/results/avgOstgMonTempDiffs")
