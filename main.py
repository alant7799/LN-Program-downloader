import pandas as pd
from bs4 import BeautifulSoup
import json
import requests
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext


sc = SparkContext.getOrCreate()
spark = SparkSession.builder.appName("Pandas to Spark").getOrCreate()

local_url = "excel_files\Muestra Anual LN+.xlsx"
processed_url = local_url.replace("\\","/")

pd_df = pd.read_excel(io=processed_url, skiprows=1, usecols="A:C")
pd_df.columns = ["week","program","link"]

spark_df = spark.createDataFrame(pd_df)

df_table_view = spark_df.createOrReplaceTempView("links")

df_select = spark.sql("SELECT regexp_replace(week, '[0-9]*[.] Semana del ', '') as week , program, link FROM links where link != 'NaN'")

links_list = df_select.toPandas().values.tolist()

dict = {}

for row in links_list:
    week, program, link = row[0], row[1], row[2]

    week = week.replace(" al", " -")
    week = week.replace(" de ", " ")

    program = program.replace('"', '')

    dict[week + " " + program] = link
    print(dict)

headers = {'resolution':'720','User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'}

for key in dict.keys():

    r = requests.get(dict[key], headers=headers, stream=True)

    soup = BeautifulSoup(r.text, 'html.parser')

    app_html = soup.find(type="application/ld+json")
    app_json = (app_html.contents)[0]
    json_for_mp4 = json.loads(app_json)

    mp4_source_link = json_for_mp4['embedUrl']

    with open(str(key) + ".mp4", 'wb') as f_out:
        r = requests.get(mp4_source_link, headers=headers, stream=True)
        print(r)
        for chunk in r.iter_content(chunk_size=1024*1024):
            if chunk:
                f_out.write(chunk)
