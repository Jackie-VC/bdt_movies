import plotly
plotly.__version__

import plotly.plotly as py
import plotly.graph_objs as go
from pyspark import SparkContext, HiveContext
from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.types import *
from pyspark.sql import SQLContext

from plotly.graph_objs import *
import pandas as pd
import requests
requests.packages.urllib3.disable_warnings()

# Create random data with numpy
import numpy as np

plotly.tools.set_credentials_file(username='RayLiang', api_key='V4mdrPCykiBiMgarXNVP')

sc = SparkContext("local[4]",appName="PythonStreamingNetworkWordCount")
sqlContext = HiveContext(sc)
df3 = sqlContext.sql("select company_id, name, count(*) as movienum from company a, movie_company b where a.id=b.company_id group by company_id, name")


trace = go.Scatter(
    x = df3.toPandas()['movienum'],
    y = df3.toPandas()['movienum'],
    name = 'company',
    mode = 'markers',
    marker = dict(
        size = 10,
        color = 'rgba(152, 0, 0, .8)',
        line = dict(
            width = 2,
            color = 'rgb(0, 0, 0)'
        )),
    text= df3.toPandas()['name']
)
data = [trace]
py.plot(data, filename="company_movie")

