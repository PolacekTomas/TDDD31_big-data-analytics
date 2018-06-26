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

# register DataFrame as a table
schemaTempReadings.registerTempTable("tempReadingsTable")

# non distinct
sqlContext.sql('SELECT year, month, count(station) as value FROM tempReadingsTable WHERE year>=1950 and year<=2014 and value>10.0 group by year, month ORDER BY value DESC').show()

# distinct
sqlContext.sql('SELECT year, month, count(distinct station) as value FROM tempReadingsTable WHERE year>=1950 and year<=2014 and value>10.0 group by year, month ORDER BY value DESC').show()
