import pandas as pd
import json
from pyspark.sql import SparkSession
import requests
from pyspark.sql.functions import explode, col, expr
from pyspark.sql.functions import arrays_zip, struct
import math
import time 


spark = SparkSession.builder \
    .appName("LH") \
    .getOrCreate()


headers = {
    "Authorization": "Bearer g36yfg4cmtdzx9nxhzyu2k95",
    "Accept": "application/json"
}

apiUrls = [
    ("https://api.lufthansa.com/v1/mds-references/airlines?limit=100&offset=0", "airlines.json", "AirlineResource", "Airlines"),
    ("https://api.lufthansa.com/v1/mds-references/airports/?limit=100&offset=0&LHoperated=1", "airport.json", "AirportResource", "Airports"),
    ("https://api.lufthansa.com/v1/mds-references/aircraft/?limit=100&offset=0", "aircraft.json", "AircraftResource", "AircraftSummaries")
]
def fetchIterateAndWrite(url, filename, resourceKey, dataKey): 
    delaySec = 1
    pageNumber = 1
    totalPages = 1

    fusedData = {}

    while pageNumber <= totalPages:
        requestUrl = f"{url}&page={pageNumber}"
        print("Fetching data from:", requestUrl)
        response = requests.get(requestUrl, headers=headers)
        print("Response status code:", response.status_code)
        dataJson = response.json()

        pageData = dataJson[resourceKey][dataKey]
        fusedData.update(pageData)
        
        
        totalCount = dataJson[resourceKey]["Meta"]["TotalCount"]
        totalPages = int(math.ceil(totalCount/100))

        pageNumber += 1
        print("Processed page", pageNumber, "of", totalPages)

        if (pageNumber <= totalPages):
            time.sleep(delaySec)

    with open(filename, "w") as writer:
       writer.write(json.dumps(fusedData))


#for url, filename, resourceKey, dataKey in apiUrls:
#    fetchIterateAndWrite(url, filename, resourceKey, dataKey)

output_path = "airline"

def transform_df(raw_df, column_mapping):
    exploded_df = raw_df.withColumn("combined", explode(arrays_zip(*column_mapping.keys())))
    transformed_df = exploded_df.select("combined.*").toDF(*column_mapping.values())
    return transformed_df

# Read airlines.json
dfRawAirline = spark.read.json("airlines.json")
column_mapping_airline = {
    "Airline.AirlineID": "AirlineID",
    "Airline.AirlineID_ICAO": "AirlineID_ICAO",
    "Airline.Names.Name.$": "Name",
    "Airline.Names.Name.@LanguageCode": "LanguageCode"
}
transformedAirlineDf = transform_df(dfRawAirline, column_mapping_airline)
transformedAirlineDf.drop("AirlineResource", "AirlineResource.meta").write.csv(output_path, mode="overwrite", header=True)



# Read airport.json
dfRawAirport = spark.read.json("airport.json")
column_mapping_airport = {
    "Airport.AirportCode": "AirportCode",
    "Airport.CityCode": "City",
    "Airport.CountryCode": "Country",
    "Airport.Position.Coordinate.Latitude": "Latitude",
    "Airport.Position.Coordinate.Longitude": "Longitude",
    "Airport.TimeZoneId": "TimeZoneId",
    "Airport.UtcOffset": "UtcOffset"
}
transformedAirportDf = transform_df(dfRawAirport, column_mapping_airport)
transformedAirportDf.write.csv("airport", mode="overwrite", header=True)

# Read aircraft.json
dfRawAircraft = spark.read.json("aircraft.json")
column_mapping_aircraft = {
    "AircraftSummary.AircraftCode": "AircraftCode",
    "AircraftSummary.AirlineEquipCode": "AirlineEquipCode",
    "AircraftSummary.Names.Name.$": "Name",
    "AircraftSummary.Names.Name.@LanguageCode": "Language"
}
transformedAircraftDf = transform_df(dfRawAircraft, column_mapping_aircraft)
transformedAircraftDf.write.csv("aircraft", mode="overwrite", header=True)




apiUrl2 = "https://api.lufthansa.com/v1/flight-schedules/flightschedules/passenger?airlines=LH&startDate=21JUN23&endDate=22JUL23&daysOfOperation=1234567&timeMode=UTC"

response = requests.get(apiUrl2, headers=headers)
dataJson = response.json()
with open("flight_schedule.json", "w") as writer:
       writer.write(json.dumps(dataJson))

dfRawSchedule = spark.read.json("flight_schedule.json")
dfRawSchedule.printSchema()

explodeLHScheduledDf = dfRawSchedule.select(
    "airline", "flightNumber", "legs", "periodOfOperationUTC.startDate", "periodOfOperationUTC.endDate"
).withColumn(
    "combined",
    explode(
        arrays_zip(
            col("legs.aircraftArrivalTimeUTC"),
            col("legs.aircraftArrivalTimeVariation"),
            col("legs.aircraftDepartureTimeUTC"),
            col("legs.aircraftOwner"),
            col("legs.aircraftType"),
            col("legs.destination"),
            col("legs.origin"),
            col("legs.registration"),
            col("legs.sequenceNumber")
        )
    )
).withColumn(
    "combined",
    struct(
        col("combined.*"),
        struct(
            col("startDate"),
            col("endDate")
        ).alias("periodOfOperationUTC")
    )
)

transformedLHScheduleDf = explodeLHScheduledDf.select("combined.*").toDF(
    "ArrivalTimeUTC", "ArrivalTimeVariation", "DepartureTimeUTC", "aircraftOwner", "aircraftType",
    "destination", "origin", "registration", "sequenceNumber", "periodOfOperationUTC"
)


scheduleDF = transformedLHScheduleDf.select(
    "*",
    expr("concat(lpad(DepartureTimeUTC div 60, 2, '0'), ':', lpad(DepartureTimeUTC % 60, 2, '0'))").alias("departureTime"),
    expr("concat(lpad(ArrivalTimeUTC div 60, 2, '0'), ':', lpad(ArrivalTimeUTC % 60, 2, '0'))").alias("arrivalTime"),
    expr("concat(lpad(ArrivalTimeVariation div 60, 2, '0'), ':', lpad(ArrivalTimeVariation % 60, 2, '0'))").alias("TimeVariation"),
).drop("ArrivalTimeVariation", "ArrivalTimeUTC", "DepartureTimeUTC", "registration")

scheduleDF.show()


