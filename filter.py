﻿
from __future__ import print_function

import sys
import json
import ast
import datetime

from pyspark import SparkContext, HiveContext
from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.types import *

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


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
def genrelation(x):
	mylist=[]
	for i in x[1]:
		mylist.append((long(x[0]),i['id']))
	#print(mylist)
	return mylist

def getSparkSessionInstance():
  if ('sparkSessionSingletonInstance' not in globals()):
    globals()['sparkSessionSingletonInstance'] = SparkSession \
      .builder \
      .appName("Python Spark SQL Hive integration example") \
      .config("hive.metastore.uris", "thrift://127.0.0.1:9083") \
      .enableHiveSupport() \
      .getOrCreate()
  return globals()['sparkSessionSingletonInstance']
	
	
if __name__ == "__main__":
  if len(sys.argv) != 3:
    print("Usage: filter.py <zk> <topic>", file=sys.stderr)
    sys.exit(-1)

  # sc = SparkContext("local[1]",appName="PythonStreamingNetworkWordCount")
  #
  # lines = sc.textFile(sys.argv[1])

  sc = SparkContext(appName="PythonStreamingKafkaWordCount")
  ssc = StreamingContext(sc, 5)

  zkQuorum, topic = sys.argv[1:]
  kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
  lines = kvs.map(lambda x: x[1].encode('utf-8'))

  # header = lines.first() #extract header
  # lines = lines.filter(lambda row : row != header)

  parts = lines.map(lambda l: l.split("\t")).filter(lambda l:len(l)==24)
  movie = parts.map(lambda p: (p[5], p[2], p[10],p[14], p[15], p[20],p[22],p[23],p[3],p[12])).filter(lambda l:l[1].isdigit() and int(l[1])>0 and l[4].isdigit() and int(l[4])>0 )
  movieIdGenre = movie.map(lambda p: (p[0], parseJson(p[8])))
  movieIdCompany = movie.map(lambda p: (p[0], parseJson(p[9])))
  genre = movieIdGenre.flatMap(lambda v: v[1][:])
  company = movieIdCompany.flatMap(lambda v: v[1][:])
  #parsed_json = json.loads("{'id': 16, 'name': Animation}")
  movie = movie.map(lambda p: (long(p[0]),long(p[1]),float(p[2]),datetime.datetime.strptime(p[3],'%m/%d/%Y').date().year, datetime.datetime.strptime(p[3],'%m/%d/%Y').date().month,long(p[4]),p[5],float(p[6]),int(p[7])))
  # genre.foreach(print)

  genre_relation = movieIdGenre.flatMap(lambda p: genrelation(p))
  company_relation = movieIdCompany.flatMap(lambda p: genrelation(p))


  # save data to hive
  # hc = getSparkSessionInstance()

  # save movie to table
  sqlContext = HiveContext(sc)

  # movieSchema = StructType([
  #   StructField("movie_id", LongType(), False),
  #   StructField("budget", LongType(), True),
  #   StructField("popularity", FloatType(), True),
  #   StructField("release_year", IntegerType(), True),
  #   StructField("release_month", IntegerType(), True),
  #   StructField("revenue", LongType(), True),
  #   StructField("title", StringType(), True),
  #   StructField("voting_score", FloatType(), True),
  #   StructField("voting_count", IntegerType(), True)
  # ])


  # Convert RDDs of the words DStream to DataFrame and run SQL query
  def processMovie(time, rdd):
    try:
      # Get the singleton instance of SparkSession
      spark = getSparkSessionInstance()

      # Convert RDD[String] to RDD[Row] to DataFrame
      existedMovieIds = sqlContext.sql("select movie_id from movie")
      existedMovieIdList = existedMovieIds.select('movie_id').rdd.flatMap(lambda x: x).collect()
      rowRdd = rdd.map(lambda p: Row(movie_id=long(p[0]), budget=long(p[1]), popularity=float(p[2]), release_year=p[3], release_month=p[4], revenue=long(p[5]), title=p[6], voting_score=float(p[7]), voting_count=float(p[8])))
      movieDF = spark.createDataFrame(rowRdd)
      newMovieDF = movieDF[~movieDF.movie_id.isin(existedMovieIdList)]
      newMovieDF.write.mode("append").saveAsTable("default.movie")
      print("========= %s =========movie saved" % str(time))
    except:
      pass

  def processGenre(time, rdd):
    try:
      # Get the singleton instance of SparkSession
      spark = getSparkSessionInstance()

      # Convert RDD[String] to RDD[Row] to DataFrame
      sqlContext.sql("CREATE TABLE IF NOT EXISTS genre(id bigint, name STRING)")
      existedGenreIds = sqlContext.sql("select id from genre")
      genreDF = spark.createDataFrame(rdd)
      existedGenreIdList = existedGenreIds.select('id').rdd.flatMap(lambda x: x).collect()
      newGenreDF = genreDF[~genreDF.id.isin(existedGenreIdList)]
      distinctedGenreDF = newGenreDF.distinct()
      distinctedGenreDF.write.mode("append").saveAsTable("default.genre")
      print("========= %s =========genre saved" % str(time))
    except:
      pass

  def processCompany(time, rdd):
    try:
      # Get the singleton instance of SparkSession
      spark = getSparkSessionInstance()

      # Convert RDD[String] to RDD[Row] to DataFrame
      sqlContext.sql("CREATE TABLE IF NOT EXISTS company(id bigint, name STRING)")
      existedCompanyIds = sqlContext.sql("select id from company")
      companyDF = spark.createDataFrame(rdd)
      existedCompanyIdList = existedCompanyIds.select('id').rdd.flatMap(lambda x: x).collect()
      newCompanyDF = companyDF[~companyDF.id.isin(existedCompanyIdList)]
      distinctedCompanyDF = newCompanyDF.distinct()
      distinctedCompanyDF.write.mode("append").saveAsTable("default.company")
      print("========= %s =========company saved" % str(time))
    except:
      pass

  def processMovieGenre(time, rdd):
    try:
      # Get the singleton instance of SparkSession
      spark = getSparkSessionInstance()

      sqlContext.sql("CREATE TABLE IF NOT EXISTS movie_genre(movie_id BIGINT, genre_id BIGINT)")
      # Convert RDD[String] to RDD[Row] to DataFrame
      rowRdd = rdd.map(lambda p: Row(movie_id=long(p[0]), genre_id=long(p[1])))
      movieGenreDF = spark.createDataFrame(rowRdd)
      movieGenreDF.write.mode("overwrite").saveAsTable("default.movie_genre")
      print("========= %s =========movie_genre saved" % str(time))
    except:
      pass

  def processMovieCompany(time, rdd):
    try:
      # Get the singleton instance of SparkSession
      spark = getSparkSessionInstance()

      sqlContext.sql("CREATE TABLE IF NOT EXISTS movie_company(movie_id BIGINT, company_id BIGINT)")
      # Convert RDD[String] to RDD[Row] to DataFrame
      rowRdd = rdd.map(lambda p: Row(movie_id=long(p[0]), company_id=long(p[1])))
      movieGenreDF = spark.createDataFrame(rowRdd)
      movieGenreDF.write.mode("overwrite").saveAsTable("default.movie_company")
      print("========= %s ==========movie_company saved" % str(time))
    except:
      pass

  movie.foreachRDD(processMovie)
  genre.foreachRDD(processGenre)
  company.foreachRDD(processCompany)
  genre_relation.foreachRDD(processMovieGenre)
  company_relation.foreachRDD(processMovieCompany)
  ssc.start()
  ssc.awaitTermination()


  # #save genre to table
  # genreSchema = StructType([
  #   StructField("id", IntegerType(), False),
  #   StructField("name", StringType(), True),
  # ])
  # genreData = genre\
  #   .flatMap(lambda v: v[1][:])
  # sqlContext.sql("CREATE TABLE IF NOT EXISTS genre(id INT, name VARCHAR(20))")
  # existedGenreIds = sqlContext.sql("select id from genre")
  # genreDF = sqlContext.createDataFrame(genreData, genreSchema)
  # existedGenreIdList = existedGenreIds.select('id').rdd.flatMap(lambda x: x).collect()
  # newGenreDF = genreDF[~genreDF.id.isin(existedGenreIdList)]
  # result = newGenreDF.distinct()
  # result.write.mode("append").saveAsTable("bdt.genre")
  #
  # #save company to table
  # companySchema = StructType([
  #   StructField("id", IntegerType(), False),
  #   StructField("name", StringType(), True),
  # ])
  # companyData = company \
  #   .flatMap(lambda v: v[1][:])
  #
  # sqlContext.sql("CREATE TABLE IF NOT EXISTS company(id INT, name VARCHAR(20))")
  # existedCompanyIds = sqlContext.sql("select id from company")
  # companyDF = sqlContext.createDataFrame(companyData, companySchema)
  # existedCompanyIdList = existedCompanyIds.select('id').rdd.flatMap(lambda x: x).collect()
  # newCompanyDF = companyDF[~companyDF.id.isin(existedCompanyIdList)]
  # result = newCompanyDF.distinct()
  # result.write.mode("append").saveAsTable("bdt.company")
  #
  #
  # #save movie_genre to table
  # movieGenreSchema = StructType([
  #   StructField("movie_id", LongType(), True),
  #   StructField("genre_id", LongType(), True),
  # ])
  # movieGenreData = genre_relation
  # sqlContext.sql("CREATE TABLE IF NOT EXISTS movie_genre(movie_id INT, genre_id INT)")
  # movieGenreDF = sqlContext.createDataFrame(movieGenreData, movieGenreSchema)
  # movieGenreDF.write.mode("append").saveAsTable("bdt.movie_genre")
  #
  #
  # #save movie_company to table
  # movieCompanySchema = StructType([
  #   StructField("movie_id", LongType(), True),
  #   StructField("company_id", LongType(), True),
  # ])
  # movieCompanyData = company_relation
  # sqlContext.sql("CREATE TABLE IF NOT EXISTS movie_company(movie_id INT, company_id INT)")
  # movieCompanyDF = sqlContext.createDataFrame(movieCompanyData, movieCompanySchema)
  # movieCompanyDF.write.mode("overwrite").saveAsTable("bdt.movie_company")

'''
  counts = lines.flatMap(lambda line: line.split("\t")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a+b)
'''
s = '{"name": "ACME", "shares": 50, "price": 490.1}'
s = '{"id": 16, "name": "Animation"}'

