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
    "Authorization": "Bearer v9fuaz6f54vtceg2wse4jpbe",
    "Accept": "application/json"
}

apiUrls = [
    ("https://api.lufthansa.com/v1/mds-references/airlines?limit=100&offset=0", "airlines.json", "AirlineResource", "Airlines"),
    ("https://api.lufthansa.com/v1/mds-references/airports/?limit=100&offset=0&LHoperated=1", "airport.json", "AirportResource", "Airports"),
    ("https://api.lufthansa.com/v1/mds-references/aircraft/?limit=100&offset=0", "aircraft.json", "AircraftResource", "AircraftSummaries"),
    ("https://api.lufthansa.com/v1/flight-schedules/flightschedules/passenger?airlines=LH&startDate=21JUN23&endDate=21JUL23&daysOfOperation=1234567&timeMode=UTC", "flight_schedule.json", "")
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


for url, filename, resourceKey, dataKey in apiUrls:
    fetchIterateAndWrite(url, filename, resourceKey, dataKey)

dfRawAirline = spark.read.json("airlines.json")
#dfRawAirline.printSchema()


explodedDf = dfRawAirline.withColumn("combined", explode(arrays_zip(col("Airline.AirlineID"),
                                                                   col("Airline.AirlineID_ICAO"),
                                                                   col("Airline.Names.Name.$"),
                                                                   col("Airline.Names.Name.@LanguageCode"))))
transformedDf = explodedDf.select("combined.*").toDF("AirlineID", "AirlineID_ICAO", "Name", "LanguageCode").drop("AirlineResource","AirlineResource.meta")

transformedDf.show()
#transformedDf.printSchema()


dfRawAirport = spark.read.json("airport.json")
#dfRawAirport.printSchema()

explodeAirportdDf = dfRawAirport.withColumn("combined", explode(arrays_zip(col("Airport.AirportCode"),
                                                                   col("Airport.CityCode"),
                                                                   col("Airport.CountryCode"),
                                                                   col("Airport.Position.Coordinate.Latitude"),
                                                                   col("Airport.Position.Coordinate.Longitude"),
                                                                   col("Airport.TimeZoneId"),
                                                                   col("Airport.UtcOffset"))))
transformedAirportDf = explodeAirportdDf.select("combined.*").toDF("AirportCode", "City", "Country", "Latitude", "Longitude", "TimeZoneId", "UtcOffset")

transformedAirportDf.show()
#transformedAirportDf.printSchema()


dfRawAircraft = spark.read.json("aircraft.json")
#dfRawAircraft.printSchema()

explodeAircraftdDf = dfRawAircraft.withColumn("combined", explode(arrays_zip(col("AircraftSummary.AircraftCode"),
                                                                   col("AircraftSummary.AirlineEquipCode"),
                                                                   col("AircraftSummary.Names.Name.$"),
                                                                   col("AircraftSummary.Names.Name.@LanguageCode"))))
transformedAircraftDf = explodeAircraftdDf.select("combined.*").toDF("AircraftCode", "AirlineEquipCode", "Name", "Language")

transformedAircraftDf.show()



apiUrl2 = "https://api.lufthansa.com/v1/flight-schedules/flightschedules/passenger?airlines=LH&startDate=21MAY23&endDate=22MAY23&daysOfOperation=1234567&timeMode=UTC"

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
