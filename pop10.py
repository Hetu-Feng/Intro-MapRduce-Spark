import sys
from pyspark import SparkContext

def country_n_pop(row):
	columns = row.split(',')

	if columns[0] in ["'COD'","'FSM'","'VGB'","'VIR'"]:
		country_ = columns[1]+columns[2]
		country = country_[2:-1]
		population = columns[7][2:-1]

	else:
		country = columns[1][2:-1]
		population = columns[6][2:-1]
	
	return (str(country),float(population))

if __name__ == "__main__":

	continent = sys.argv[1]

	sc = SparkContext.getOrCreate()
	lines = sc.textFile('country.csv')

	result  = lines.filter(lambda row: continent in row) \
				.map(country_n_pop) \
				.map(lambda x: (x[0],x[1])).collect()

	result.sort(key = lambda x: x[1], reverse = True)

	out = open("q2_a.txt","w+")
	for pair in result[:10]:
		print(pair[0]+', '+str(pair[1])+'\n')
		out.write(pair[0]+', '+str(pair[1])+'\n')
	out.close()   