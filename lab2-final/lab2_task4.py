from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName="maxMin")
sqlContext = SQLContext(sc)

# read the temperature data
rdd = sc.textFile("data/temperature-readings.csv")
# create DataFrame from RDD
parts = rdd.map(lambda a: a.split(';'))
tempReadingsRow = parts.map(lambda x: (x[0], x[1], int(x[1].split("-")[0]), int(x[1].split("-")[1]), x[2], float(x[3]), x[4]))
tempReadingsString = ["station", "date", "year", "month", "time", "value", "quality"]
schemaTempReadings = sqlContext.createDataFrame(tempReadingsRow, tempReadingsString)

# read the precipitation data
rdd = sc.textFile("data/precipitation-readings.csv")
# create DataFrame from RDD
parts = rdd.map(lambda a: a.split(';'))
precReadingsRow = parts.map(lambda x: (x[0], x[1], int(x[1].split("-")[0]), int(x[1].split("-")[1]), x[2], float(x[3]), x[4]))
precReadingsString = ["station", "date", "year", "month", "time", "value", "quality"]
schemaPrecReadings = sqlContext.createDataFrame(precReadingsRow, precReadingsString)

# find the max temp per station
maxTemps = schemaTempReadings.groupBy('station').agg(F.max('value').alias('maxTemp'))
# filter
maxTemps = maxTemps.filter((maxTemps['maxTemp'] >= 25) & (maxTemps['maxTemp'] <= 30))

# calculate the daily precipitation and find the max
maxPrecs = schemaPrecReadings.groupBy('station', 'date').agg(F.sum('value')).groupBy('station').agg(F.max('sum(value)').alias('maxDailyPrecipitation'))
# filter
maxPrecs = maxPrecs.filter((maxPrecs['maxDailyPrecipitation'] >= 100) & (maxPrecs['maxDailyPrecipitation'] <= 200))

# join and output the max temp and max precipitation
joined = maxTemps.join(maxPrecs, 'station', 'inner').orderBy('station', ascending=False).show()