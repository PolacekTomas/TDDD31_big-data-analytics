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

# read the Ostergotland stations data
rdd = sc.textFile("data/stations-Ostergotland.csv")
# create DataFrame from RDD
parts = rdd.map(lambda a: a.split(';'))
statOstRow = parts.map(lambda x: (x[0], x[1]))
statOstString = ["station", "name"]
schemaStatOst = sqlContext.createDataFrame(statOstRow, statOstString)

# join to filter for ostg stations
schemaTempReadings = schemaTempReadings.join(schemaStatOst, 'station', 'inner')

# filter by year
schemaTempReadings = schemaTempReadings.filter( (schemaTempReadings['year'] >= 1950) & (schemaTempReadings['year'] <= 2014) )

# calulate the average
minMaxTemps = schemaTempReadings.groupby(['station', 'date', 'year', 'month']).agg(F.min('value'), F.max('value'))

# calulate first the daily average then the monthly average from the daily average
avgMonthlyTemps = minMaxTemps.withColumn( 'dailyAvg', (minMaxTemps['min(value)']+minMaxTemps['max(value)'])/2.0 ).groupBy('year', 'month', 'station').agg(F.avg('dailyAvg').alias('avgMonthlyTempPerStat')).groupBy('year', 'month').agg(F.avg('avgMonthlyTempPerStat').alias('avgMonthlyTemperature'))

# calculate the long term average for the period from 1950-1980
longTermAvgTemp = avgMonthlyTemps.filter(avgMonthlyTemps['year'] <= 1980).groupBy('month').agg(F.avg('avgMonthlyTemperature').alias('longTermAvgTemp'))

# calculate the difference between the monthly average and the long term average
diff = avgMonthlyTemps.join(longTermAvgTemp, 'month', 'inner')
diff = diff.withColumn('difference', diff['avgMonthlyTemperature']-diff['longTermAvgTemp']).select('year', 'month', 'difference').orderBy(['year', 'month'], ascending=[0, 0]).show()