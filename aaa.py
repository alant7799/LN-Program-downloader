import pandas as pd
from bs4 import BeautifulSoup
import json
import requests

local_url = "excel_files\Muestra Anual LN+.xlsx"

processed_url = local_url.replace("\\","/")
print(processed_url)                                  

#archivo_csv = pd.read_excel(io=processed_url)