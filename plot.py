import plotly
plotly.__version__

import plotly.plotly as py
import plotly.graph_objs as go
from pyspark import SparkContext, HiveContext
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

sc = SparkContext("local[4]",appName="PythonStreamingNetworkWordCount")
sqlContext = HiveContext(sc)
df2 = sqlContext.sql("select title, budget, revenue, (revenue-budget) as profit from movie where budget>10000")

#data = Data([Histogram(x=df2.toPandas()['budget'],y=df2.toPandas()['revenue'])])

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

'''
N = 1000

trace = go.Scatter(
    x = np.random.randn(N),
    y = np.random.randn(N)+2,
    name = 'Above',
    mode = 'markers',
    marker = dict(
        size = 10,
        color = 'rgba(152, 0, 0, .8)',
        line = dict(
            width = 2,
            color = 'rgb(0, 0, 0)'
        )
    )
)

data = [trace]

layout = dict(title = 'Styled Scatter',
              yaxis = dict(zeroline = False),
              xaxis = dict(zeroline = False)
             )

fig = dict(data=data, layout=layout)

# Plot and embed in ipython notebook!
py.plot(fig, filename='basic-scatter')
'''