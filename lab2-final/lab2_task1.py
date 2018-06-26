from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName="maxMin")
sqlContext = SQLContext(sc)

# read the temperature data
rdd = sc.textFile("data/temperature-readings.csv").cache()
# create DataFrame from RDD
parts = rdd.map(lambda a: a.split(';'))
tempReadingsRow = parts.map(lambda x: (x[0], x[1], int(x[1].split("-")[0]), int(x[1].split("-")[1]), x[2], float(x[3]), x[4]))
tempReadingsString = ["station", "date", "year", "month", "time", "value", "quality"]
schemaTempReadings = sqlContext.createDataFrame(tempReadingsRow, tempReadingsString)

# filter data by years
schemaTempReadings = schemaTempReadings.filter( (schemaTempReadings['year'] >= 1950) & (schemaTempReadings['year'] <= 2014) )
# find the minimum temperature for each year
schemaTempReadingsMin = schemaTempReadings.groupBy('year').agg(F.min('value').alias('value'))
# join the data to print station number
schemaTempReadingsMin = schemaTempReadingsMin.join(schemaTempReadings, ['year', 'value'], 'inner').select('year', 'station', 'value').orderBy('value', ascending=False).show()

# find the maximum temperature for each year
schemaTempReadingsMax = schemaTempReadings.groupBy('year').agg(F.max('value').alias('value'))
# join the data to print station number
schemaTempReadingsMax = schemaTempReadingsMax.join(schemaTempReadings, ['year', 'value'], 'inner').select('year', 'station', 'value').orderBy('value', ascending=False).show()
