import os
import json
import requests

from datetime import datetime

"""
Idea with OOP 

class MLAPI:
    BASE_URL = 'https://api.mercadolibre.com/sites/MLA/'

    def __init__(self):
        pass

    def get(self, relative):
        """
        Example relative: 'search?category=MLA1000'
        """
        pass
    
    def save(self)
        pass
"""

BASE_URL = 'https://api.mercadolibre.com/sites/MLA/'
relative = 'search?category=MLA1000'

# concat base with relative path
url = f"{BASE_URL}{relative}" 
# get data
response = requests.get(url)

# create directory with dinamic date
time = datetime.now()
time_string = time.strftime(f"%Y%m")
directory = f"challenges/searchjson{time_string}"

try:
    os.makedirs(directory)
except:
    pass

# save response data to directory
fs = open(f"{directory}/response.json", "w") 
json.dump(response.json(), fs, indent=6) 
fs.close()
