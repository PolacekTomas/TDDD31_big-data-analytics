from pyspark import SparkContext

sc = SparkContext(appName="countOver10")
#lines = sc.textFile("/user/x_pauth/data/temperature-readings.csv").cache()
lines = sc.textFile("data/temperature-readings.csv").cache()
lines = lines.map(lambda a: a.split(';'))
lines = lines.filter(lambda x: int(x[1][0:4]) >= 1950 and int(x[1][0:4]) <= 2014)
lines = lines.filter(lambda x: float(x[3]) > 10)

stations = lines.map(lambda x: (x[0]+';'+x[1][:7], 1))
stations = stations.reduceByKey(lambda v1, v2: v1)

over10 = stations.map(lambda x: (x[0].split(';')[1], 1))

cntOver10 = over10.reduceByKey(lambda v1, v2: v1+v2)
#cntOver10.saveAsTextFile("/user/x_pauth/results/cntOver10StationsUnique")
cntOver10.saveAsTextFile("results/cntOver10StationsUnique")
