import pandas as pd
import json
import math
import time
import requests
from datetime import datetime
import os 
import ExtractFunctionsDST

# List of URL fro API call, file name and data Key 
apiUrls = [
    ("https://api.lufthansa.com/v1/mds-references/airlines?limit=100&offset=0", "./data/extractedjson/airlines.json", "AirlineResource", "Airlines"),
    ("https://api.lufthansa.com/v1/mds-references/airports/?limit=100&offset=0&LHoperated=1", "./data/extractedjson/airport.json", "AirportResource", "Airports"),
    ("https://api.lufthansa.com/v1/mds-references/aircraft/?limit=100&offset=0", "./data/extractedjson/aircraft.json", "AircraftResource", "AircraftSummaries")
]

# Information on the headers for API calls
headers = {
    "Authorization": "Bearer urbsgx2u2k9mz4tw7xmtd633",
    "Accept": "application/json"
}

"""""
for url, filename, resource_key, data_key in apiUrls:
   ExtractFunctionsDST.fetch_iterate_and_write(url, filename, resource_key, data_key)
"""""

# Processing of Airline Data 
fileName = "./data/extractedjson/airlines.json"
airlines_df = ExtractFunctionsDST.process_airline_data(fileName)
print(airlines_df)


# Processing of Airport Data 
fileName = "./data/extractedjson/airport.json"
airport_df = ExtractFunctionsDST.process_airport_data(fileName)
print(airport_df)


# Processing of Aircraft Data 
fileName = "./data/extractedjson/aircraft.json"
aircraft_df = ExtractFunctionsDST.process_aircraft_data(fileName)
print(aircraft_df)


apiUrl2 = "https://api.lufthansa.com/v1/flight-schedules/flightschedules/passenger?airlines=LH&startDate=21JUN23&endDate=22JUL23&daysOfOperation=1234567&timeMode=UTC"

fileName = "./data/extractedjson/flight_schedule.json"

# Make the HTTP request and save the data to a JSON file
response = requests.get(apiUrl2, headers=headers)
dataJson = response.json()
with open(fileName, "w") as writer:
    writer.write(json.dumps(dataJson))

#Processing of Flight Schedule Data 
schedule_df = ExtractFunctionsDST.process_flight_schedule(fileName)
print(schedule_df)

routeDf = schedule_df[['origin', 'destination']]

# Get unique combinations of 'origin' and 'destination'
unique_combinations = routeDf.drop_duplicates()

# Extract unique values from 'origin' column
origin = unique_combinations['origin'].tolist()

# Extract unique values from 'destination' column
destination = unique_combinations['destination'].tolist()

today = datetime.now().date()
formattedDate = today.strftime("%Y-%m-%d")
fileName = f'./data/extractedjson/flight_status1{formattedDate}.json'

# Retrieval of Flight Status from the day 
response_flight_info = ExtractFunctionsDST.fetch_flight_information(origin, destination, formattedDate, headers)

# Write a file from the Flight Status API 
ExtractFunctionsDST.write_responses_to_file(response_flight_info, fileName)

# Process the Flight Status Data 
flight_status_df = ExtractFunctionsDST.process_flight_status_data(fileName)
print(flight_status_df)

