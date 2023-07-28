import pandas as pd
import json
import math
import time
import requests
from datetime import datetime
import os 

headers = {
    "Authorization": "Bearer urbsgx2u2k9mz4tw7xmtd633",
    "Accept": "application/json"
    }



def fetch_iterate_and_write(url, filename, resource_key, data_key):
    """
    Load JSON data from a file into a Pandas DataFrame.

    Args:
        file_path (str): Path to the JSON file.
        key (str): Key corresponding to the desired data within the JSON file.

    Returns:
        pandas.DataFrame: DataFrame containing the JSON data, or an empty DataFrame if the key is not found.
    """

    delay_sec = 1
    page_number = 1
    total_pages = 1
    fused_data = {}
    
    while page_number <= total_pages:
        request_url = f"{url}&page={page_number}"
        print("Fetching data from:", request_url)
        response = requests.get(request_url, headers=headers)
        print("Response status code:", response.status_code)
        data_json = response.json()
        page_data = data_json[resource_key][data_key]
        fused_data.update(page_data)
        total_count = data_json[resource_key]["Meta"]["TotalCount"]
        total_pages = math.ceil(total_count / 100)
        page_number += 1
        print("Processed page", page_number, "of", total_pages)
        if page_number <= total_pages:
            time.sleep(delay_sec)

    with open(filename, "w") as writer:
        writer.write(json.dumps(fused_data))


def load_json_to_dataframe(file_path, key):
    """

    """
    with open(file_path, "r") as file:
        data = json.load(file)
        if key in data:
            return pd.DataFrame(data[key])
        else:
            return pd.DataFrame()

def convert_to_time(minutes):
    """
    Convert time from Minutes format to HH:MM

    Args: 
        minutes (int): Number of minutes

    Returns:
        str: Formatted time string in the format HH:MM
    """
    hours = minutes // 60
    minutes = minutes % 60
    return f"{hours:02d}:{minutes:02d}"

def transform_df(raw_df, column_mapping):
    """
    Transform a raw DataFrame by renaming columns and extracting information from the 'Names' column.

    Args:
        raw_df (pd.DataFrame): Raw DataFrame to be transformed.
        column_mapping (dict): Mapping of original column names to new column names.

    Returns:
        pd.DataFrame: Transformed DataFrame.
    """
    transformed_df = raw_df.copy()
    transformed_df['Names'] = transformed_df['Names'].str['Name'].str['$']
    transformed_df.rename(columns=column_mapping, inplace=True)

    return transformed_df


def process_airline_data(filename):
    """
    Process airline data from a JSON file by transforming it and saving the result to a CSV file.

    Args:
        filename (str): Path to the JSON file.

    Returns:
        pd.DataFrame: Processed airline data as a Pandas DataFrame.
    """
    # Read airlines.json
    output_file = "./data/extractedcsv/airline.csv"
    with open(filename, "r") as file:
        data = json.load(file)
        df_raw_airline = pd.DataFrame(data['Airline'])

    # Column mapping for transformation
    column_mapping_airline = {
        "AirlineID": "AirlineID",
        "AirlineID_ICAO": "AirlineID_ICAO",
        "$": "Name"
    }

    # Apply transformation to the DataFrame
    transformed_airline_df = transform_df(df_raw_airline, column_mapping_airline)

    # Write transformed DataFrame to CSV
    transformed_airline_df.to_csv(output_file, index=False)

    return transformed_airline_df


def process_flight_schedule(filename):
    """
    Process flight schedule data from a JSON file.

    Args:
        filename (str): Path to the JSON file.

    Returns:
        pd.DataFrame: Processed flight schedule data as a Pandas DataFrame.
    """
    with open(filename) as file:
        dataJson = json.load(file)
    
    # Normalize the JSON data
    dfRawSchedule = pd.json_normalize(dataJson)
    
    # Explode the nested legs array
    dfRawSchedule = dfRawSchedule.explode('legs')
    
    # Select relevant columns and flatten the nested periodOfOperationUTC
    dfRawSchedule = dfRawSchedule[['airline', 'flightNumber', 'legs', 'periodOfOperationUTC.startDate', 'periodOfOperationUTC.endDate']]
    
    dfRawSchedule[['origin', 'destination', 'aircraftOwner', 'aircraftType', 'aircraftArrivalTimeUTC', 'aircraftDepartureTimeUTC', 'aircraftArrivalTimeVariation']] = dfRawSchedule['legs'].apply(
        lambda x: pd.Series([x['origin'], x['destination'], x['aircraftOwner'], x['aircraftType'], x['aircraftArrivalTimeUTC'], x['aircraftDepartureTimeUTC'], x['aircraftArrivalTimeVariation']]) 
        if isinstance(x, dict) else [None, None]
    )
    dfCleanSchedule = dfRawSchedule.drop(['legs'], axis=1)
    
    dfCleanSchedule = dfCleanSchedule.rename(columns={
        'periodOfOperationUTC.startDate': 'startDate',
        'periodOfOperationUTC.endDate': 'endDate'
    })
    dfCleanSchedule['departureTime'] = dfCleanSchedule['aircraftDepartureTimeUTC'].apply(convert_to_time)
    dfCleanSchedule['arrivalTime'] = dfCleanSchedule['aircraftArrivalTimeUTC'].apply(convert_to_time)
    dfCleanSchedule['TimeVariation'] = dfCleanSchedule['aircraftArrivalTimeVariation'].apply(convert_to_time)
    
    scheduleDF = dfCleanSchedule[['airline', 'flightNumber', 'arrivalTime', 'TimeVariation', 'departureTime', 'aircraftOwner',
                            'aircraftType', 'destination', 'origin', 'startDate', 'endDate']]

    scheduleDF.to_csv("./data/extractedcsv/flight_schedule.csv", index=False)

    return scheduleDF


def process_airport_data(filename):
    """
    Process airport data from a JSON file.

    Args:
        filename (str): Path to the JSON file.

    Returns:
        pd.DataFrame: Processed airport data as a Pandas DataFrame.
    """
    with open(filename, "r") as file:
        data = json.load(file)
    
    df_raw_airport = pd.DataFrame(data['Airport'])
    
    column_mapping_airport = {
        "AirportCode": "AirportCode",
        "Names": "Names",
        "CityCode": "City",
        "CountryCode": "Country",
        "Position": "Latitude",
        "Position": "Longitude",
        "TimeZoneId": "TimeZoneId",
        "UtcOffset": "UtcOffset"
    }
    
    transformed_airport_df = df_raw_airport.copy()
    transformed_airport_df[['Latitude', 'Longitude']] = transformed_airport_df['Position'].apply(
        lambda x: pd.Series([x['Coordinate']['Latitude'], x['Coordinate']['Longitude']]) 
        if isinstance(x, dict) else [None, None]
    )
    transformed_airport_df.drop(['Position', 'LocationType'], axis=1, inplace=True)
    
    transformed_airport_df["Names"] = transformed_airport_df["Names"].apply(lambda x: x["Name"])
    transformed_airport_df["Names"] = transformed_airport_df["Names"].apply(lambda x: [item["$"] for item in x if isinstance(item, dict) and "$" in item] if isinstance(x, list) else None)
    transformed_airport_df["Names"] = transformed_airport_df["Names"].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None)
    
    transformed_airport_df.rename(columns=column_mapping_airport, inplace=True)
    
    transformed_airport_df.to_csv("./data/extractedcsv/airport.csv", index=False)
    
    return transformed_airport_df


def process_aircraft_data(filename):
    """
    Process aircraft data from a JSON file.

    Args:
        filename (str): Path to the JSON file.

    Returns:
        pd.DataFrame: Processed aircraft data as a Pandas DataFrame.
    """
    with open(filename, "r") as file:
        data = json.load(file)
    
    df_raw_aircraft = pd.DataFrame(data['AircraftSummary'])
    
    column_mapping_aircraft = {
        "AircraftCode": "AircraftCode",
        "AirlineEquipCode": "Name",
        "Name": "AirlineEquipCode",
    }
    
    transformed_aircraft_df = df_raw_aircraft.copy()
    transformed_aircraft_df['Names'] = transformed_aircraft_df['Names'].str['Name'].str['$']
    transformed_aircraft_df.rename(columns=column_mapping_aircraft, inplace=True)
    
    transformed_aircraft_df.to_csv("./data/extractedcsv/aircraft.csv", index=False)
    
    return transformed_aircraft_df


def process_flight_status_data(filename):
    """
    Process flight status data from a JSON file and save it to a CSV file.

    Args:
        filename (str): Path to the JSON file.

    Returns:
        None
    """
    today = datetime.now().date()
    formattedDate = today.strftime("%Y-%m-%d")

    with open(filename, 'r') as file:
        data = json.load(file)
    
    # Extract the 'FlightInformation' data
    df_raw_status = pd.json_normalize(data['responses'])
    
    # Explode the 'Flights' array
    df_raw_status = df_raw_status.explode('FlightInformation.Flights.Flight')
    
    # Flatten the nested JSON structure
    df_raw_status = pd.concat([df_raw_status.drop(['FlightInformation.Flights.Flight.Departure.Estimated.Time', 
                                                   'FlightInformation.Meta.@Version', 'FlightInformation.Meta.Link', 
                                                   'FlightInformation.Meta.TotalCount', 'FlightInformation.Flights.Flight',
                                                   'FlightInformation.Flights.Flight.Departure.Terminal.Name', 'FlightInformation.Flights.Flight.Departure.Terminal.Gate',
                                                   'FlightInformation.Flights.Flight.Arrival.Estimated.Time', 'FlightInformation.Flights.Flight.Arrival.Estimated.Date',
                                                   'FlightInformation.Flights.Flight.MarketingCarrierList.MarketingCarrier',
                                                   'FlightInformation.Flights.Flight.Arrival.Terminal.Name', 'FlightInformation.Flights.Flight.Arrival.Terminal.Gate',
                                                   'FlightInformation.Flights.Flight.Arrival.Estimated.Date', 'FlightInformation.Flights.Flight.Arrival.Estimated.Time',
                                                   'FlightInformation.Flights.Flight.Status.Code', 'FlightInformation.Flights.Flight.Departure.Estimated.Date'], axis=1),
                              df_raw_status['FlightInformation.Flights.Flight'].apply(pd.Series)], axis=1)
    
    df_raw_status_1 = df_raw_status[['Departure', 'Arrival', 'MarketingCarrierList', 'OperatingCarrier', 'Equipment', 'Status']]

    df_raw_status_1[['DepAirportCode', 'DepSchedDate', 'DepSchedTime', 'DepActualTime', 'DepStatusCode', 'DepStatusDesc']] = df_raw_status_1['Departure'].apply(
        lambda x: pd.Series([
            x['AirportCode'],
            x['Scheduled']['Date'],
            x['Scheduled']['Time'],
            x['Actual']['Time'] if 'Actual' in x and 'Time' in x['Actual'] else None,
            x['Status']['Code'],
            x['Status']['Description']
        ]) if isinstance(x, dict) else pd.Series([None, None, None, None, None])
    )

    df_raw_status_1[['ArrivalAirportCode', 'ArrSchedDate', 'ArrSchedTime', 'ArrActualTime', 'ArrActualDate', 'ArrStatusCode', 'ArrStatusDesc']] = df_raw_status_1['Arrival'].apply(
        lambda x: pd.Series([
            x['AirportCode'],
            x['Scheduled']['Date'],
            x['Scheduled']['Time'],
            x['Actual']['Time'] if 'Actual' in x and 'Time' in x['Actual'] else None,
            x['Actual']['Date'] if 'Actual' in x and 'Date' in x['Actual'] else None,
            x['Status']['Code'],
            x['Status']['Description']
        ]) if isinstance(x, dict) else pd.Series([None, None, None, None, None])
    )

    df_raw_status_1[['OpAirlineID', 'OpFlightNumber']] = df_raw_status_1['OperatingCarrier'].apply(
        lambda x: pd.Series([
            x['AirlineID'],
            x['FlightNumber']
        ]) if isinstance(x, dict) else pd.Series([None, None])
    )

    df_raw_status_1[['AircraftCode']] = df_raw_status_1['Equipment'].apply(
        lambda x: pd.Series([
            x['AircraftCode']
        ]) if isinstance(x, dict) else pd.Series([None])
    )

    df_raw_status_1.drop(['Departure', 'Arrival', 'OperatingCarrier', 'MarketingCarrierList', 'Equipment', 'Status'], axis=1, inplace=True)

    df_raw_status = df_raw_status.rename(columns={
        'FlightInformation.Flights.Flight.Departure.AirportCode': 'DepAirportCode',
        'FlightInformation.Flights.Flight.Departure.Scheduled.Date': 'DepSchedDate',
        'FlightInformation.Flights.Flight.Departure.Scheduled.Time': 'DepSchedTime',
        'FlightInformation.Flights.Flight.Departure.Actual.Date': 'DepActualDate',
        'FlightInformation.Flights.Flight.Departure.Actual.Time': 'DepActualTime',
        'FlightInformation.Flights.Flight.Departure.Status.Code': 'DepStatusCode',
        'FlightInformation.Flights.Flight.Departure.Status.Description': 'DepStatusDesc',
        'FlightInformation.Flights.Flight.Arrival.AirportCode': 'ArrivalAirportCode',
        'FlightInformation.Flights.Flight.Arrival.Scheduled.Date': 'ArrSchedDate',
        'FlightInformation.Flights.Flight.Arrival.Scheduled.Time': 'ArrSchedTime',
        'FlightInformation.Flights.Flight.Arrival.Actual.Time': 'ArrActualTime',
        'FlightInformation.Flights.Flight.Arrival.Actual.Date': 'ArrActualDate',
        'FlightInformation.Flights.Flight.Arrival.Status.Code': 'ArrStatusCode',
        'FlightInformation.Flights.Flight.Arrival.Status.Description': 'ArrStatusDesc',
        'FlightInformation.Flights.Flight.Equipment.AircraftCode': 'AircraftCode',
        'FlightInformation.Flights.Flight.MarketingCarrierList.MarketingCarrier.AirlineID': 'MarkAirlineID',
        'FlightInformation.Flights.Flight.MarketingCarrierList.MarketingCarrier.FlightNumber': 'MarkFlightNumber',
        'FlightInformation.Flights.Flight.OperatingCarrier.AirlineID': 'OpAirlineID',
        'FlightInformation.Flights.Flight.OperatingCarrier.FlightNumber': 'OpFlightNumber'
    }).drop(['FlightInformation.Flights.Flight.Status.Description', 'Departure', 'Arrival', 'MarketingCarrierList', 'OperatingCarrier', 'Equipment', 'Status', 0], axis=1)

    df_raw_status.dropna()
    df_raw_status_1.dropna()
    df_merged_status = df_raw_status.merge(df_raw_status_1, how='outer')

    df_merged_status.dropna(how='all', inplace=True)
    df_merged_status.to_csv(f"./data/extractedcsv/flight_status{formattedDate}.csv", index=False)
    return df_merged_status


def fetch_flight_information(origin, destination, formatted_date, headers):
    """
    Fetch flight information from the Lufthansa API for each combination of origin and destination.

    Args:
        origin (list): List of origin airport codes.
        destination (list): List of destination airport codes.
        formatted_date (str): Formatted date string.
        headers (dict): Headers for API requests.

    Returns:
        list: List of API responses.
    """
    responses_list = []

    for i in range(len(origin)):
        if origin[i] != destination[i]:
            status_url = f'https://api.lufthansa.com/v1/operations/customerflightinformation/route/{origin[i]}/{destination[i]}/{formatted_date}'
            
            try:
                response = requests.get(status_url, headers=headers)
                response.raise_for_status()
                data_json = response.json()
                responses_list.append(data_json)
            except requests.exceptions.HTTPError as e:
                print(f"HTTP Error occurred: {str(e)}")
                print(f"Status code: {response.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"An error occurred while making the API request: {str(e)}")
            except json.JSONDecodeError as e:
                print(f"An error occurred while parsing the JSON response: {str(e)}")

    return responses_list


def write_responses_to_file(responses_list, file_name):
    """
    Write the accumulated responses to a JSON file.

    Args:
        responses_list (list): List of API responses.
        file_name (str): Name of the output JSON file.

    Returns:
        None
    """
    with open(file_name, "w") as writer:
        writer.write(json.dumps({"responses": responses_list}))

    with open(file_name, 'rb+') as file:
        file.seek(-1, os.SEEK_END)
        file.truncate()
        file.write(b'}')