import requests 
import time
import os 
import io
import pandas as pd

url_datos = "https://data.cityofnewyork.us/resource/n2zq-pubd.json"

url_api = os.getenv("API_URL", "http://api:5000") + "/ingest/911_calls"

