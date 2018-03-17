
from __future__ import print_function

import sys
import json
import ast
import datetime

from pyspark import SparkContext, HiveContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def parseJson(s):
	s = ast.literal_eval(s)
	a = json.dumps(s)
	b = json.loads(a)
	return b
	
def parseGenre(x):
	print("movid = ",x[0])
'''
	
	for i in x[1]:
		print('genreid=',i['id'], " name=",i['name'])
	return 
'''
	
def parseCompany(x):

	print("movid = ",x[0])
'''
	
	for i in x[1]:
		print('companyid=',i['id'], " name=",i['name'])

	return 
'''
	
if __name__ == "__main__":
  if len(sys.argv) != 2:
    print("Usage: filter.py <filename>", file=sys.stderr)
    sys.exit(-1)
    
  sc = SparkContext("local[1]",appName="PythonStreamingNetworkWordCount")
  
  lines = sc.textFile(sys.argv[1])
  header = lines.first() #extract header
  lines = lines.filter(lambda row : row != header) 

  parts = lines.map(lambda l: l.split("\t")).filter(lambda l:len(l)==24)
  movie = parts.map(lambda p: (p[5], p[2], p[10],p[14], p[15], p[20],p[22],p[23],p[3],p[12])).filter(lambda l:l[1].isdigit() and int(l[1])>0 and l[4].isdigit() and int(l[4])>0 )
  genre = movie.map(lambda p: (p[0], parseJson(p[8])))
  company = movie.map(lambda p: (p[0], parseJson(p[9])))
	#parsed_json = json.loads("{'id': 16, 'name': Animation}")
  movie = movie.map(lambda p: (long(p[0]),long(p[1]),float(p[2]),datetime.datetime.strptime(p[3],'%m/%d/%Y').date().year, datetime.datetime.strptime(p[3],'%m/%d/%Y').date().month,long(p[4]),p[5],float(p[6]),int(p[7])))
  movie.foreach(print)
  #genre.foreach(parseGenre)
  #company.foreach(parseCompany)


  # save movie to hive
  hc = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("hive.metastore.uris", "thrift://127.0.0.1:9083") \
    .enableHiveSupport() \
    .getOrCreate()

  # save movie to table
  sqlContext = HiveContext(sc)
  movieSchema = StructType([
    StructField("movie_id", LongType(), False),
    StructField("budget", LongType(), True),
    StructField("popularity", FloatType(), True),
    StructField("release_year", IntegerType(), True),
    StructField("release_month", IntegerType(), True),
    StructField("revenue", LongType(), True),
    StructField("title", StringType(), True),
    StructField("voting_score", FloatType(), True),
    StructField("voting_count", IntegerType(), True)
  ])
  movieDF = sqlContext.createDataFrame(movie, schema=movieSchema)
  movieDF.write.mode("overwrite").saveAsTable("default.movie")
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

