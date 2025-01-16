import pandas as pd
from shapely.geometry import LineString
import psycopg2

class PostgreSQL:
    """
    A class for managing PostgreSQL database operations, including inserting data at 5-minute and 1-hour intervals,
    deleting tables, and handling database connections.

    Attributes:
        conn (psycopg2.extensions.connection): Connection object for PostgreSQL.
        cursor (psycopg2.extensions.cursor): Cursor object for executing SQL queries.
    """

    def __init__(self):
        """
        Initializes the PostgreSQL connection and cursor.
        """
        self.conn = psycopg2.connect(
            host="163.143.165.154",
            database="postgres",
            user="postgres",
            password="postgres"
        )
        self.cursor = self.conn.cursor()

    def delete(self, operation):
        """
        Deletes specified tables from the database based on the operation type.

        Args:
            operation (str): Specifies which tables to delete. Valid options are "5min", "1hour", "geography", or "all".
        """
        if operation == "5min" or operation == "all":
            self.cursor.execute('DROP TABLE IF EXISTS congestion')
            self.cursor.execute('DROP TABLE IF EXISTS road')
            self.cursor.execute('DROP TABLE IF EXISTS regulation')
            self.conn.commit()

        if operation == "1hour" or operation == "all":
            self.cursor.execute('DROP TABLE IF EXISTS congestion_1hour')
            self.cursor.execute('DROP TABLE IF EXISTS road_1hour')
            self.conn.commit()

        if operation == "geography" or operation == "all":
            self.cursor.execute('DROP TABLE IF EXISTS geography')
            self.conn.commit()

    def insert_5min(self, file_path_5min):
        """
        Inserts 5-minute interval data into corresponding tables.

        Args:
            file_path_5min (str): Path to the CSV file containing 5-minute interval data.
        """
        chunk_size = 1000

        # Create tables
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS congestion (
            id SERIAL PRIMARY KEY,
            offer_date VARCHAR,
            offer_day BIGINT,
            offer_hour BIGINT,
            event_no VARCHAR,
            congestion_degree DOUBLE PRECISION,
            congestion_length DOUBLE PRECISION
        )''')

        self.cursor.execute('''CREATE TABLE IF NOT EXISTS road (
            id SERIAL PRIMARY KEY,
            pref_no INT,
            course_no INT,
            course_name VARCHAR,
            dir_name VARCHAR,
            low_kp DOUBLE PRECISION,
            low_latitude DOUBLE PRECISION,
            low_longitude DOUBLE PRECISION,
            low_altitude DOUBLE PRECISION,
            low_spot_name VARCHAR,
            low_cityname_code VARCHAR,
            up_kp DOUBLE PRECISION,
            up_latitude DOUBLE PRECISION,
            up_longitude DOUBLE PRECISION,
            up_altitude DOUBLE PRECISION,
            up_spot_name VARCHAR,
            up_cityname_code VARCHAR
        )''')

        self.cursor.execute('''CREATE TABLE IF NOT EXISTS regulation (
            id SERIAL PRIMARY KEY,
            time VARCHAR,
            event_no VARCHAR,
            event_seq NUMERIC,
            regulation VARCHAR,
            link_distance NUMERIC,
            reason VARCHAR
        )''')

        self.conn.commit()

        for chunk in pd.read_csv(file_path_5min, sep="\t", low_memory=False, chunksize=chunk_size):
            chunk.rename(columns={'Unnamed: 0': 'id'}, inplace=True)

            # Insert into congestion table
            congestion_data = chunk[["id", "offer_date", "offer_day", "offer_hour", "event_no", "congestion_degree", "congestion_length"]].values.tolist()
            congestion_query = '''INSERT INTO congestion (
                id, offer_date, offer_day, offer_hour, event_no, congestion_degree, congestion_length
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING'''
            self.cursor.executemany(congestion_query, congestion_data)

            # Insert into road table
            road_data = chunk[["id", "pref_no", "course_no", "course_name", "dir_name", "low_kp", "low_latitude",
                               "low_longitude", "low_altitude", "low_spot_name", "low_cityname_code", "up_kp",
                               "up_latitude", "up_longitude", "up_altitude", "up_spot_name", "up_cityname_code"]].values.tolist()
            road_query = '''INSERT INTO road (
                id, pref_no, course_no, course_name, dir_name, low_kp, low_latitude, low_longitude, low_altitude,
                low_spot_name, low_cityname_code, up_kp, up_latitude, up_longitude, up_altitude, up_spot_name, up_cityname_code
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING'''
            self.cursor.executemany(road_query, road_data)

            # Insert into regulation table
            regulation_data = chunk[["id", "offer_date", "event_no", "event_seq", "regulation", "link_distance", "reason"]]
            regulation_data = regulation_data.rename(columns={"offer_date": "time"}).values.tolist()
            regulation_query = '''INSERT INTO regulation (
                id, time, event_no, event_seq, regulation, link_distance, reason
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING'''
            self.cursor.executemany(regulation_query, regulation_data)

        self.conn.commit()

    def insert_1hour(self, file_path_1hour):
        """
        Inserts 1-hour interval data into corresponding tables.

        Args:
            file_path_1hour (str): Path to the CSV file containing 1-hour interval data.
        """
        chunk_size = 1000

        # Create tables
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS congestion_1hour (
            id SERIAL PRIMARY KEY,
            time VARCHAR,
            allCount NUMERIC,
            lightCongestion NUMERIC,
            heavyCongestion NUMERIC,
            averageLength NUMERIC,
            maxLength NUMERIC,
            congestionTime NUMERIC,
            congestionAmount NUMERIC,
            linkLength NUMERIC
        )''')

        self.cursor.execute('''CREATE TABLE IF NOT EXISTS road_1hour (
            id SERIAL PRIMARY KEY,
            roadName VARCHAR,
            direction VARCHAR,
            dwLocation VARCHAR,
            dwLatitude DOUBLE PRECISION,
            dwLongitude DOUBLE PRECISION,
            upLocation VARCHAR,
            upLatitude DOUBLE PRECISION,
            upLongitude DOUBLE PRECISION
        )''')

        self.conn.commit()

        for chunk in pd.read_csv(file_path_1hour, chunksize=chunk_size):
            if 'id' not in chunk.columns:
                chunk['id'] = range(1, len(chunk) + 1)

            # Insert into congestion_1hour table
            congestion_data = chunk[["id", "time", "allCount", "lightCongestion", "heavyCongestion",
                                    "averageLength", "maxLength", "congestionTime", "congestionAmount", "linkLength"]].values.tolist()
            congestion_query = '''INSERT INTO congestion_1hour (
                id, time, allCount, lightCongestion, heavyCongestion, averageLength, maxLength, congestionTime,
                congestionAmount, linkLength
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING'''
            self.cursor.executemany(congestion_query, congestion_data)

            # Insert into road_1hour table
            road_data = chunk.rename(columns={"ColumnInCSV": "roadName"})[[
                "id", "roadName", "direction", "dwLocation", "dwLatitude", "dwLongitude",
                "upLocation", "upLatitude", "upLongitude"
            ]].values.tolist()
            road_query = '''INSERT INTO road_1hour (
                id, roadName, direction, dwLocation, dwLatitude, dwLongitude, upLocation, upLatitude, upLongitude
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING'''
            self.cursor.executemany(road_query, road_data)

        self.conn.commit()

    def close(self):
        """
        Closes the database connection and cursor.
        """
        self.cursor.close()
        self.conn.close()
