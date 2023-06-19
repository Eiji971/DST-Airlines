import pandas as pd
import requests
import json
from pyspark.sql import SparkSession
import requests

spark = SparkSession.builder \
    .appName("LH") \
    .getOrCreate()

url = "https://api.lufthansa.com/v1/mds-references/airlines?limit=20&offset=0"
headers = {
    "Authorization": "Bearer 6d4rmazx5bnu9jy6fejtxpzf",
    "Accept": "application/json"
}

response = requests.get(url, headers=headers)
airportJson = response.json()

dfRawAirport = spark.read.json(spark.sparkContext.parallelize([airportJson]))
dfRawAirport.show()
dfRawAirport.printSchema()
