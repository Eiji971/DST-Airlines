import pandas as pd
import json
import math
import time
import requests
from datetime import datetime
import os 
import ExtractFunctionsDST
from Authentication_key_retrieval import get_valid_token

# List of URL fro API call, file name and data Key 
apiUrls = [
    ("https://api.lufthansa.com/v1/mds-references/airlines?limit=100&offset=0", "/data/extractedjson/airlines.json", "AirlineResource", "Airlines"),
    ("https://api.lufthansa.com/v1/mds-references/airports/?limit=100&offset=0&LHoperated=1", "/data/extractedjson/airport.json", "AirportResource", "Airports"),
    ("https://api.lufthansa.com/v1/mds-references/aircraft/?limit=100&offset=0", "/data/extractedjson/aircraft.json", "AircraftResource", "AircraftSummaries")
]


bearer_token = get_valid_token()
# Information on the headers for API calls
headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Accept": "application/json"
    }



for url, filename, resource_key, data_key in apiUrls:
   ExtractFunctionsDST.fetch_iterate_and_write(url, filename, resource_key, data_key)



# Processing of Airline Data 
fileName = "/data/extractedjson/airlines.json"
airlines_df = ExtractFunctionsDST.process_airline_data(fileName)
print(airlines_df)


# Processing of Airport Data 
fileName = "/data/extractedjson/airport.json"
airport_df = ExtractFunctionsDST.process_airport_data(fileName)
print(airport_df)


# Processing of Aircraft Data 
fileName = "/data/extractedjson/aircraft.json"
aircraft_df = ExtractFunctionsDST.process_aircraft_data(fileName)
print(aircraft_df)