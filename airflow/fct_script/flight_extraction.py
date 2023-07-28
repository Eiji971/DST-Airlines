import pandas as pd
import json
import math
import time
import requests
from datetime import datetime
import os 
import ExtractFunctionsDST

headers = {
    "Authorization": "Bearer urbsgx2u2k9mz4tw7xmtd633",
    "Accept": "application/json"
}

apiUrl2 = "https://api.lufthansa.com/v1/flight-schedules/flightschedules/passenger?airlines=LH&startDate=21JUN23&endDate=22JUL23&daysOfOperation=1234567&timeMode=UTC"

fileName = "./data/extractedjson/flight_schedule.json"

# Make the HTTP request and save the data to a JSON file
response = requests.get(apiUrl2, headers=headers)
dataJson = response.json()
with open(fileName, "w") as writer:
    writer.write(json.dumps(dataJson))
    
def get_schedule(fileName): 
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

