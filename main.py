import pandas as pd
from bs4 import BeautifulSoup
import json
import requests

# specify the URL of the archive here
url = 'https://cdn.jwplayer.com/videos/k0LzXTYu-kTExGaWf.mp4'
headers = {'resolution':'720','User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'}
    
with open('video.mp4', 'wb') as f_out:
    r = requests.get(url, headers=headers, stream=True)
    print(r)
    for chunk in r.iter_content(chunk_size=1024*1024):
        if chunk:
            f_out.write(chunk)