import pandas as pd
import json
from pyspark.sql import SparkSession
import requests
from pyspark.sql.functions import explode, col
from pyspark.sql.functions import arrays_zip




spark = SparkSession.builder \
    .appName("LH") \
    .getOrCreate()

url = "https://api.lufthansa.com/v1/mds-references/airlines?limit=100&offset=0"
headers = {
    "Authorization": "Bearer 6d4rmazx5bnu9jy6fejtxpzf",
    "Accept": "application/json"
}

response = requests.get(url, headers=headers)
airportJson = response.json()

dfRawAirport = spark.read.json(spark.sparkContext.parallelize([airportJson]))

dfRawAirport.printSchema()


explodedDf = dfRawAirport.withColumn("combined", explode(arrays_zip(col("AirlineResource.Airlines.Airline.AirlineID"),
                                                                   col("AirlineResource.Airlines.Airline.AirlineID_ICAO"),
                                                                   col("AirlineResource.Airlines.Airline.Names.Name.$"),
                                                                   col("AirlineResource.Airlines.Airline.Names.Name.@LanguageCode"))))
transformedDf = explodedDf.select("combined.*").toDF("AirlineID", "AirlineID_ICAO", "Name", "LanguageCode").drop("AirlineResource","AirlineResource.meta")
transformedDf.show()
transformedDf.printSchema()