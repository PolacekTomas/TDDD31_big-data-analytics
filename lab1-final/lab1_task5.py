from pyspark import SparkContext

sc = SparkContext(appName="avgPrecOstg")

# read the list of stations in Ostergotland
stationsOstg = sc.textFile("data/stations-Ostergotland.csv").cache()
#stationsOstg = sc.textFile("/user/x_pauth/data/stations-Ostergotland.csv").cache()
# collect the data to have it as a local variable
stationsOstg = stationsOstg.map(lambda a: a.split(';')[0]).collect()

precipitation = sc.textFile("data/precipitation-readings.csv").cache()
#precipitation = sc.textFile("/user/x_pauth/data/precipitation-readings.csv").cache()
precipitation = precipitation.map(lambda a: a.split(';'))
precipitation = precipitation.filter(lambda x: int(x[1][0:4]) >= 1993 and int(x[1][0:4]) <= 2016)
# filter with regard to the stations location (Ostergotland)
precipitation = precipitation.filter(lambda x: x[0] in stationsOstg)
# calculate the monthly precipitation for each station
precipitation = precipitation.map(lambda x: (x[0]+';'+x[1][:7], float(x[3])))
precipitation = precipitation.reduceByKey(lambda v1,v2: v1+v2)

# calculate the average monthly precipitation
precipitation = precipitation.map(lambda x: (x[0].split(';')[1], (x[1],1)))
precipitation = precipitation.reduceByKey(lambda v1,v2: (v1[0]+v2[0], v1[1]+v2[1]))
precipitation = precipitation.map(lambda x: (x[0], x[1][0]/x[1][1]))

# save the results
precipitation.saveAsTextFile("results/avgOstgMonPrec")
#precipitation.saveAsTextFile("/user/x_pauth/results/avgOstgMonPrec")







