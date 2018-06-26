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
schemaTempReadings = schemaTempReadings.filter( (schemaTempReadings['year'] >= 1960) & (schemaTempReadings['year'] <= 2014) )

minMaxTemps = schemaTempReadings.groupby(['station', 'date', 'year', 'month']).agg(F.min('value'), F.max('value'))

avgTemps = minMaxTemps.withColumn( 'dailyAvg', (minMaxTemps['min(value)']+minMaxTemps['max(value)'])/2.0 ).groupBy('year', 'month', 'station').agg(F.avg('dailyAvg').alias('avgMonthlyTemperature')).orderBy('avgMonthlyTemperature', ascending=False).show()
