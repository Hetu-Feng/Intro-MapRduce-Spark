from pyspark import SparkContext
import sys


def unofficialLan (row):
	columns = row.split(',')
	if columns[2] == " 'T'":
		code = columns[0]
		unofficial = 0
	else:
		code = columns[0]
		unofficial = 1
	return (code,unofficial)


def code_to_name(row):
	columns = row.split(',')

	if columns[0] in code_only.keys():
		if columns[0] in ["'COD'","'FSM'","'VGB'","'VIR'"]:
			country = columns[1][2:-1]+', '+columns[2][1:-1]
			return (float(code_only[columns[0]]), country)
		else:
			country = columns[1][2:-1]
			return (float(code_only[columns[0]]), country)


if __name__ == "__main__":
    
    sc = SparkContext.getOrCreate()

    lines_country = sc.textFile("country.csv")
    lines_language = sc.textFile("countrylanguage.csv")

    counts = lines_language.map(unofficialLan)\
    		.reduceByKey(lambda x, y: x + y)\
    		.filter(lambda x: x[1]>=10)

    code_reday = counts.collect()
    # code_reday.sort(key = lambda x:x[1], reverse=True)

    code_only = {}
    for i in code_reday:
    	code_only[i[0]] = i[1]

    result = lines_country.map(code_to_name)\
    		.filter(lambda x: x != None)\
    		.sortByKey(False)\
    		.collect()

    out = open('q2_c.txt','w+')
    for i in result:
    	if i:
            print(i[1]+'\n')
            out.write(i[1]+'\n')
    out.close()




