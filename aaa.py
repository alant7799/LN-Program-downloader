import pandas as pd
from bs4 import BeautifulSoup
import json
import requests

headers = {'resolution':'720','User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'}

r = requests.get(url)

soup = BeautifulSoup(r.text, 'html.parser')

app_html = soup.find(type="application/ld+json")
app_json = (app_html.contents)[0]
json_for_mp4 = json.loads(app_json)

mp4_source_link = json_for_mp4['embedUrl']

print(mp4_source_link)
"""
with open('video.mp4', 'wb') as f_out:
    r = requests.get(url, headers=headers, stream=True)
    print(r)
    for chunk in r.iter_content(chunk_size=1024*1024):
        if chunk:
            f_out.write(chunk)
"""
