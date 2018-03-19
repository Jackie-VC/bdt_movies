import plotly
plotly.__version__

import plotly.plotly as py
import plotly.graph_objs as go
from pyspark import SparkContext, HiveContext
from pyspark.sql import SparkSession, Row, DataFrame

from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.types import *
from pyspark.sql import SQLContext

import plotly.plotly as py
from plotly.graph_objs import *
import pandas as pd
import requests
requests.packages.urllib3.disable_warnings()

# Create random data with numpy
import numpy as np

plotly.tools.set_credentials_file(username='RayLiang', api_key='V4mdrPCykiBiMgarXNVP')

def getSparkSessionInstance():
	if ('sparkSessionSingletonInstance' not in globals()):
		globals()['sparkSessionSingletonInstance'] = SparkSession \
			.builder \
			.appName("Python Spark SQL Hive integration example") \
			.config("hive.metastore.uris", "thrift://127.0.0.1:9083") \
			.enableHiveSupport() \
			.getOrCreate()
	return globals()['sparkSessionSingletonInstance']

def createProfit(sqlContext):
	df2 = sqlContext.sql("select title, budget, revenue, (revenue-budget) as profit from movie where budget>10000")
	trace = go.Scatter(
	    x = df2.toPandas()['budget'],
	    y = df2.toPandas()['profit'],
	    name = 'Profit',
	    mode = 'markers',
	    marker = dict(
	        size = 10,
	        color = 'rgba(152, 0, 0, .8)',
	        line = dict(
	            width = 2,
	            color = 'rgb(0, 0, 0)'
	        )),
	    text= df2.toPandas()['title']
	)
	data = [trace]

	py.plot(data, filename="best profit")
	return

def createCompany(sqlContext):
	df = sqlContext.sql("select company_id, name, count(*) as movienum from company a, movie_company b where a.id=b.company_id group by company_id, name")
	trace = go.Scatter(
	    x = df.toPandas()['movienum'],
	    y = df.toPandas()['movienum'],
	    name = 'company',
	    mode = 'markers',
	    marker = dict(
	        size = 10,
	        color = 'rgba(152, 0, 0, .8)',
	        line = dict(
	            width = 2,
	            color = 'rgb(0, 0, 0)'
	        )),
	    text= df.toPandas()['name']
	)
	data = [trace]
	py.plot(data, filename="company_movie")
	return

def createCompanyMovie(sqlContext):
	df = sqlContext.sql("select release_year, sum(budget) as bud, sum(revenue) as rev, count(*) as num from movie a, movie_company b where a.movie_id=b.movie_id and company_id=6194 and budget>10000 and release_year>=1970 group by release_year")
	trace1 = go.Bar(
    x=df.toPandas()['release_year'],
    y=df.toPandas()['bud'],
    text= df.toPandas()['num'],
    name='Budget'
	)
	trace2 = go.Bar(
    x=df.toPandas()['release_year'],
    y=df.toPandas()['rev'],
	  name='Revenue'
	)

	data = [trace1, trace2]
	layout = go.Layout(
	    barmode='group'
	)

	fig = go.Figure(data=data, layout=layout)
	py.plot(fig, filename='Warner Bro')
	return

# sc = SparkContext("local[4]",appName="PythonStreamingNetworkWordCount")
# sqlContext = HiveContext(sc)

sqlContext = getSparkSessionInstance()
createProfit(sqlContext)
createCompany(sqlContext)
createCompanyMovie(sqlContext)
