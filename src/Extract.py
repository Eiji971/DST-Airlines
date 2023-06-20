import pandas as pd
import json
from pyspark.sql import SparkSession
import requests
from pyspark.sql.functions import explode, col
from pyspark.sql.functions import arrays_zip
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

dfRawAirline = spark.read.json("airlines.json")

dfRawAirline.printSchema()


explodedDf = dfRawAirline.withColumn("combined", explode(arrays_zip(col("Airline.AirlineID"),
                                                                   col("Airline.AirlineID_ICAO"),
                                                                   col("Airline.Names.Name.$"),
                                                                   col("Airline.Names.Name.@LanguageCode"))))
transformedDf = explodedDf.select("combined.*").toDF("AirlineID", "AirlineID_ICAO", "Name", "LanguageCode").drop("AirlineResource","AirlineResource.meta")

transformedDf.show()
transformedDf.printSchema()


dfRawAirport = spark.read.json("airport.json")

dfRawAirport.printSchema()

explodeAirportdDf = dfRawAirport.withColumn("combined", explode(arrays_zip(col("Airport.AirportCode"),
                                                                   col("Airport.CityCode"),
                                                                   col("Airport.CountryCode"),
                                                                   col("Airport.Position.Coordinate.Latitude"),
                                                                   col("Airport.Position.Coordinate.Longitude"),
                                                                   col("Airport.TimeZoneId"),
                                                                   col("Airport.UtcOffset"))))
transformedAirportDf = explodeAirportdDf.select("combined.*").toDF("AirportCode", "City", "Country", "Latitude", "Longitude", "TimeZoneId", "UtcOffset")

transformedAirportDf.show()
transformedAirportDf.printSchema()


dfRawAircraft = spark.read.json("aircraft.json")
dfRawAircraft.printSchema()

explodeAircraftdDf = dfRawAircraft.withColumn("combined", explode(arrays_zip(col("AircraftSummary.AircraftCode"),
                                                                   col("AircraftSummary.AirlineEquipCode"),
                                                                   col("AircraftSummary.Names.Name.$"),
                                                                   col("AircraftSummary.Names.Name.@LanguageCode"))))
transformedAircraftDf = explodeAircraftdDf.select("combined.*").toDF("AircraftCode", "AirlineEquipCode", "Name", "Language")

transformedAircraftDf.show()
