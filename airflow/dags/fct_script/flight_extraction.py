import pandas as pd
import json
import math
import time
import requests
from datetime import datetime
import os 
import fct_script.ExtractFunctionsDST
import fct_script.Authentication_key_retrieval
client_id = 'pppgbsjxaegfhhh5ehjjgstnb'
client_secret = '6aHXhkBTH6'


bearer_token = fct_script.Authentication_key_retrieval.get_bearer_token(client_id, client_secret)

headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Accept": "application/json"
    }

apiUrl2 = "https://api.lufthansa.com/v1/flight-schedules/flightschedules/passenger?airlines=LH&startDate=21JUN23&endDate=22JUL23&daysOfOperation=1234567&timeMode=UTC"
    
def get_schedule(fileName): 
    #Processing of Flight Schedule Data 
    schedule_df = fct_script.ExtractFunctionsDST.process_flight_schedule(fileName)
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
    fileName = f'./dataclean/flight_status{formattedDate}.json'

    # Retrieval of Flight Status from the day 
    response_flight_info = fct_script.ExtractFunctionsDST.fetch_flight_information(origin, destination, formattedDate, headers)

    # Write a file from the Flight Status API 
    fct_script.ExtractFunctionsDST.write_responses_to_file(response_flight_info, fileName)

    # Process the Flight Status Data 
    flight_status_df = fct_script.ExtractFunctionsDST.process_flight_status_data(fileName)

