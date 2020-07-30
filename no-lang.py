from pyspark import SparkContext
import sys

def get_name(row):
	columns = row.split(',')
	if columns[0] in countryCode:
		return columns[1][2:-1]


if __name__ == "__main__":


	sc = SparkContext.getOrCreate()
	lines_country = sc.textFile("country.csv")
	lines_language = sc.textFile("countrylanguage.csv")

	countryCode = lines_country.map(lambda row: row.split(',')[0])\
					.subtract(lines_language.map(lambda row: row.split(',')[0])).collect()

	result = lines_country.map(get_name).collect()

	out = open("q2_b.txt", "w+")
	for item in result:
		if item:
			print(item+'\n')
			out.write(item+'\n')
	out.close()
