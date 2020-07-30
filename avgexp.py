from pyspark import SparkContext
import sys


def get_c_g_l(row):

	columns = row.split(',')
	if columns[0] in ["'COD'","'FSM'","'VGB'","'VIR'"]:
		continent = columns[3][2:-1]
		lifeExpectancy = columns[8][2:-1]
		gnp = columns[9][2:-1]

	else:
		continent = columns[2][2:-1]
		lifeExpectancy = columns[7][2:-1]
		gnp = columns[8][2:-1]

	if  gnp == "":
		gnp = 0
	
	if lifeExpectancy == "":
		lifeExpectancy = 0
	
	return (continent, float(gnp), float(lifeExpectancy))


if __name__ == "__main__":
 
	sc = SparkContext.getOrCreate()
	
	lines = sc.textFile("country.csv")

	result = lines.map(get_c_g_l)\
		     .filter(lambda x: x[1] > 10000)\
		     .map(lambda x: (x[0], x[2]))\
		     .groupByKey()\
		     .filter(lambda x: len(x[1]) >= 5)\
		     .map(lambda x: (x[0], sum(x[1])/len(x[1]))).collect()


	out = open("q2_d.txt", "w+")
	for pair in result:
		print(pair[0] + ', ' + str(pair[1]) + '\n')
		out.write(pair[0] + ', ' + str(pair[1]) + '\n')
	out.close()

