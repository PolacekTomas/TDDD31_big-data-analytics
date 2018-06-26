from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName="maxMin")
sqlContext = SQLContext(sc)

# read the precipitation data
rdd = sc.textFile("data/precipitation-readings.csv")
# create DataFrame from RDD
parts = rdd.map(lambda a: a.split(';'))
precReadingsRow = parts.map(lambda x: (x[0], x[1], int(x[1].split("-")[0]), int(x[1].split("-")[1]), x[2], float(x[3]), x[4]))
precReadingsString = ["station", "date", "year", "month", "time", "value", "quality"]
schemaPrecReadings = sqlContext.createDataFrame(precReadingsRow, precReadingsString)

# read the Ostergotland stations data
rdd = sc.textFile("data/stations-Ostergotland.csv")
# create DataFrame from RDD
parts = rdd.map(lambda a: a.split(';'))
statOstRow = parts.map(lambda x: (x[0], x[1]))
statOstString = ["station", "name"]
schemaStatOst = sqlContext.createDataFrame(statOstRow, statOstString)

#filter by year
schemaPrecReadings = schemaPrecReadings.filter( (schemaPrecReadings['year'] >= 1993) & (schemaPrecReadings['year'] <= 2016) )

# join to filter for ostg stations
schemaPrecReadings = schemaPrecReadings.join(schemaStatOst, 'station', 'inner')

# monthly precipitation
schemaPrecReadings.groupBy('station', 'year', 'month').agg(F.sum('value')).groupBy('year', 'month').agg(F.avg('sum(value)').alias('avgMonthlyPrecipitation')).orderBy(['year', 'month'], ascending=[0, 0]).show()
