import pandas as pd
from bs4 import BeautifulSoup
import json
import requests
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from datetime import datetime, date
from pyspark.sql.functions import current_date,year

sc=SparkContext.getOrCreate()
spark = SparkSession.builder.appName("Pandas to Spark").getOrCreate()

local_url = "excel_files\Muestra Anual LN+.xlsx"

processed_url = local_url.replace("\\","/")

pd_df = pd.read_excel(io=processed_url, skiprows=1, usecols="A:C")

spark_df = spark.createDataFrame(pd_df)

spark_df.printSchema()
spark_df.show()



# specify the URL of the archive here
""""url = 'https://cdn.jwplayer.com/videos/k0LzXTYu-kTExGaWf.mp4'
headers = {'resolution':'720','User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'}
    
with open('video.mp4', 'wb') as f_out:
    r = requests.get(url, headers=headers, stream=True)
    print(r)
    for chunk in r.iter_content(chunk_size=1024*1024):
        if chunk:
            f_out.write(chunk)
"""