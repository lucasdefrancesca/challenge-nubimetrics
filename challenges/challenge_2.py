import os
import json
import requests

from datetime import datetime


class MLAPI:
    BASE_URL = 'https://api.mercadolibre.com/sites/MLA/'

    def __init__(self):
        self.response = None

    def get(self, relative):
        url = f"{self.BASE_URL}{relative}"
        try:
            self.response = requests.get(url)
        except requests.exceptions.HTTPError as e:
            return f"Error: {e}"

    def save(self):
        time = datetime.now()
        time_string = time.strftime(f"%Y%m")
        directory = f"challenges/searchjson{time_string}"

        try:
            os.makedirs(directory)
            # save response data to directory
            fs = open(f"{directory}/response.json", "w") 
            json.dump(self.response.json(), fs, indent=6) 
            fs.close()
        except:
            pass


api_ml = MLAPI()
api_ml.get('search?category=MLA1000')
api_ml.save()
