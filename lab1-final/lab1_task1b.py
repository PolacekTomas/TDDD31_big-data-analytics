from __future__ import print_function
import time

# measure time
start = time.time()

# initialize variables
maxTemperatures = {}
minTemperatures = {}
for year in range(1950, 2014+1):
	maxTemperatures[year] = (float("-inf"), "")
	minTemperatures[year] = (float("+inf"), "")

# process file  line by line to find max and min per year
with open('data/temperature-readings.csv') as f:
	cnt = 0
	for line in f:
		vals = line.split(';')
		year = int(vals[1][:4])
		if year >= 1950 and year <= 2014:
			if maxTemperatures[year][0] < float(vals[3]):
				maxTemperatures[year] = (float(vals[3]), vals[0])
			if minTemperatures[year][0] > float(vals[3]):
				minTemperatures[year] = (float(vals[3]), vals[0])
		cnt += 1
		if cnt % 100000 == 0:
			print("\rread line #{}".format(cnt), end='')

# output the progress
print("\rread line #{}".format(cnt))

# sort and print the results
maxTemperaturesSorted = sorted(maxTemperatures.items(), key=lambda tup: tup[1][0], reverse=True)
minTemperaturesSorted = sorted(minTemperatures.items(), key=lambda tup: tup[1][0], reverse=True)
print("Maximum Temperatures:")
print ("\n".join(map(str, maxTemperaturesSorted)))
print("Minimum Temperatures:")
print ("\n".join(map(str, minTemperaturesSorted)))

# print the execution time
print("Time needed: {:.2f}s".format(time.time()-start))
