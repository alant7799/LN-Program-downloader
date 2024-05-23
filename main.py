import os
import pandas as pd
from bs4 import BeautifulSoup
import json
import requests
from pyspark.sql import SparkSession
from pyspark import SparkContext

local_url = r"excel_files\Muestra Anual LN+.xlsx"
output_url = r"C:\Users\Alan\Downloads"

"""replaces backslash for forward slash"""
def process_url(url):
    return url.replace("\\","/")

"""recibe url de un archivo excel donde se consumen las 3 primeras columnas"""
def extract(local_url):

    sc = SparkContext.getOrCreate()
    spark = SparkSession.builder.appName("Pandas to Spark").getOrCreate()

    processed_url = process_url(local_url)

    pd_df = pd.read_excel(io=processed_url, skiprows=1, usecols="A:C")
    pd_df.columns = ["week","program","link"]

    spark_df = spark.createDataFrame(pd_df)

    df_table_view = spark_df.createOrReplaceTempView("links")

    df_select = spark.sql("SELECT regexp_replace(week, '[0-9]*[.] Semana del ', '') as week , program, link FROM links where link != 'NaN'")

    return df_select.toPandas().values.tolist()

""" recibe una lista de listas [[week, program, link]...], la limpia"""
def clean_list(links_list):

    dict = {}

    for row in links_list:

        week, program, link = row[0], row[1], row[2]

        week = week.replace(" al", " -")
        week = week.replace(" de ", " ")

        program = program.replace('"', '')

        dict[week + " " + program] = link

    return dict

""" recibe dict donde las claves son el nombre y semana del programa y el valor el link a la pagina"""
def get_mp4_source_as_dict_values(dict):

    for key in dict.keys():

        r = requests.get(dict[key])

        soup = BeautifulSoup(r.text, 'html.parser')

        app_html = soup.find(type="application/ld+json")
        app_json = (app_html.contents)[0]
        json_for_mp4 = json.loads(app_json)

        mp4_source_link = json_for_mp4['embedUrl']

        dict[key] = mp4_source_link
    
    return dict

""" recibe una lista de listas [[week, program, link]...]"""
def transform(links_list):

    dict = clean_list(links_list)

    return get_mp4_source_as_dict_values(dict)

""" recibe nombre a dar al archivo mp4 y link de source """
def get_mp4_files(file_name, link, output_url):

    headers = {'resolution':'720','User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'}

    with open(os.path.join(output_url, file_name + ".mp4"), "wb") as f_out:
        r = requests.get(link, headers=headers, stream=True)
        print(r)
        for chunk in r.iter_content(chunk_size=1024*1024):
            if chunk:
                f_out.write(chunk)

""" recibe dict con claves nombre y semana programa, valor source mp4, los descarga"""
def load(dict, output_url):

    for key in dict.keys():

        get_mp4_files(key,dict[key], output_url)

""" funcion principal, recibe url local de archivo excel a consumir"""
def main(local_url, output_url):

    rows_list = extract(local_url)

    programs_dict = transform(rows_list)

    load(programs_dict, output_url)

main(local_url, output_url)