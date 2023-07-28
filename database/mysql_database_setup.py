import mysql.connector
from mysql.connector import errorcode
import pandas as pd

def create_table_schema(table_schema):
    try:
        # Connect to the MySQL server 
        conn = mysql.connector.connect(
            host='127.0.0.1',
            port='3307',
            user='root',
            password='Lufthansa',
            database='sys',
        )

        # Create a cursor to execute SQL statements
        cursor = conn.cursor()

        # Execute the table creation query
        cursor.execute(table_schema)

        # Commit the changes to the database
        conn.commit()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Error: Access denied. Check your username and password.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Error: Database does not exist.")
        else:
            print(f"Error: {err}")
    finally:
        # Close the cursor and the connection
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()



airline_table_schema = (
    "CREATE TABLE IF NOT EXISTS Airline ("
    "  AirlineID VARCHAR(10) PRIMARY KEY,"
    "  AirlineID_ICAO VARCHAR(45) NOT NULL,"
    "  Name VARCHAR(45) NOT NULL"
    ")"
)

aircraft_table_schema = (
    "CREATE TABLE IF NOT EXISTS Aircraft ("
    "  AircraftCode VARCHAR(6) PRIMARY KEY,"
    "  Names VARCHAR(45) NOT NULL,"
    "  Name VARCHAR(10) NOT NULL"
    ")"
)

airport_table_schema = (
    "CREATE TABLE IF NOT EXISTS Airport ("
    "  AirportCode VARCHAR(15) PRIMARY KEY,"
    "  City VARCHAR(45) NOT NULL,"
    "  Country VARCHAR(45) NOT NULL,"
    "  Names VARCHAR(45) NOT NULL,"
    "  UtcOffset VARCHAR(45) NOT NULL,"
    "  TimeZoneId VARCHAR(45) NOT NULL,"
    "  Latitude FLOAT NOT NULL,"
    "  Longitude FLOAT NOT NULL"
    ")"
)



def ingest_data_from_dataframe(filepath, table_name):
    try:
        # Connect to the MySQL server
        conn = mysql.connector.connect(
            host='127.0.0.1',
            port='3307',
            user='root',
            password='Lufthansa',
            database='sys',
        )

        dataframe = pd.read_csv(filepath)

        # Create a cursor to execute SQL statements
        cursor = conn.cursor()

        # Prepare the placeholders for the INSERT query
        placeholders = ', '.join(['%s'] * len(dataframe.columns))

        # Generate the INSERT query
        insert_query = f"INSERT IGNORE INTO {table_name} ({', '.join(dataframe.columns)}) VALUES ({placeholders})"

        # Insert data into the table
        for _, row in dataframe.iterrows():
            values = tuple(row.values)
            cursor.execute(insert_query, values)

        # Commit the changes to the database
        conn.commit()

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

create_table_schema(airport_table_schema)
create_table_schema(airline_table_schema)
create_table_schema(aircraft_table_schema)

ingest_data_from_dataframe('/Users/emerybosc/DST-Airlines/data/extractedcsv/aircraft.csv', 'Airport')
ingest_data_from_dataframe('/Users/emerybosc/DST-Airlines/data/extractedcsv/aircraft.csv', 'Airline')
ingest_data_from_dataframe('/Users/emerybosc/DST-Airlines/data/extractedcsv/aircraft.csv', 'Aircraft')