
from __future__ import print_function

import sys
import json
import ast

from pyspark import SparkContext, HiveContext
from pyspark.sql.types import *

def parseJson(s):
	s = ast.literal_eval(s)
	a = json.dumps(s)
	b = json.loads(a)
	return b
	
def parseGenre(x):
	print("movid = ",x[0])
	
	for i in x[1]:
		print('genreid=',i['id'], " name=",i['name'])
		
	return 
	
def parseCompany(x):
	print("movid = ",x[0])
	
	for i in x[1]:
		print('companyid=',i['id'], " name=",i['name'])
		
	return 
	
if __name__ == "__main__":
  if len(sys.argv) != 2:
    print("Usage: filter.py <filename>", file=sys.stderr)
    sys.exit(-1)
  sc = SparkContext("local[1]",appName="PythonStreamingNetworkWordCount")
  
  lines = sc.textFile(sys.argv[1])
  parts = lines.map(lambda l: l.split("\t")).filter(lambda l:len(l)==24)
  movie = parts.map(lambda p: (p[5], p[2], p[10],p[14], p[15], p[20],p[22],p[23])).filter(lambda l:int(l[1])>0)
  genre = parts.map(lambda p: (p[5], parseJson(p[3])))
  company = parts.map(lambda p: (p[5], parseJson(p[12])))
	#parsed_json = json.loads("{'id': 16, 'name': Animation}")
  movie.foreach(print)
  genre.foreach(parseGenre)
  company.foreach(parseCompany)


  # save movie to table
  sqlContext = HiveContext(sc)
  movieSchema = StructType([
    StructField("movie_id", StringType(), False),
    StructField("budget", StringType(), True),
    StructField("popularity", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("revenue", StringType(), True),
    StructField("title", StringType(), True),
    StructField("voting_score", StringType(), True),
    StructField("voting_count", StringType(), True)
  ])
  movieDF = sqlContext.createDataFrame(movie, schema=movieSchema)
  movieDF.write.mode("append").saveAsTable("default.movie")
  result = sqlContext.sql("select * from movie")
  print("=========================")
  result.show()


'''
  counts = lines.flatMap(lambda line: line.split("\t")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a+b)
'''
s = '{"name": "ACME", "shares": 50, "price": 490.1}'
s = '{"id": 16, "name": "Animation"}'

