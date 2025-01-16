
import pandas as pd
import csv
import os
import psycopg2

class PostgreSQL:
    """
    A class for managing distributed PostgreSQL database operations. Includes functionalities for
    creating, inserting, and deleting data in both 5-minute and 1-hour interval tables.

    Attributes:
        conn (psycopg2.extensions.connection): Connection to the PostgreSQL database.
        cursor (psycopg2.extensions.cursor): Cursor for executing SQL queries.
    """

    def __init__(self):
        """
        Initializes the PostgreSQL connection and cursor.
        """
        self.conn = psycopg2.connect(
            host='163.143.165.154',
            database='distributed',
            user='postgres',
            password='postgres'
        )
        self.cursor = self.conn.cursor()

    def delete(self, operation):
        """
        Deletes specified tables from the database.

        Args:
            operation (str): Specifies which tables to delete. Options:
                - "5min": Deletes tables related to 5-minute data.
                - "1hour": Deletes tables related to 1-hour data.
                - "all": Deletes all related tables.
        """
        # Define table lists for 5min and 1hour operations
        tables_5min = ["congestion", "road", "regulation"]
        tables_1hour = ["congestion_1hour", "road_1hour"]

        # Drop 5min-related tables if applicable
        if operation in ("5min", "all"):
            for table in tables_5min:
                self.cursor.execute(f'DROP TABLE IF EXISTS {table} CASCADE')
            self.conn.commit()

        # Drop 1hour-related tables if applicable
        if operation in ("1hour", "all"):
            for table in tables_1hour:
                self.cursor.execute(f'DROP TABLE IF EXISTS {table} CASCADE')
            self.conn.commit()

    def insert_5min(self, file_path_5min):
        """
        Creates and inserts data into 5-minute interval tables.

        Args:
            file_path_5min (str): Path to the 5-minute interval TSV data file.
        """
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
        self.cursor.execute("SELECT create_distributed_table('congestion', 'id')")

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
        self.cursor.execute("SELECT create_distributed_table('road', 'id')")

        self.cursor.execute('''CREATE TABLE IF NOT EXISTS regulation (
            id SERIAL PRIMARY KEY,
            time VARCHAR,
            event_no VARCHAR,
            event_seq NUMERIC,
            regulation VARCHAR,
            link_distance NUMERIC,
            reason VARCHAR
        )''')
        self.cursor.execute("SELECT create_distributed_table('regulation', 'id')")
        self.conn.commit()

        # Insert data
        chunk_size = 10000
        for chunk in pd.read_csv(file_path_5min, sep="\t", low_memory=False, chunksize=chunk_size):
            chunk.rename(columns={'Unnamed: 0': 'id'}, inplace=True)

            # Insert into tables
            self.insert_data("congestion", chunk[["id", "offer_date", "offer_day", "offer_hour", "event_no", 
                                                  "congestion_degree", "congestion_length"]].values.tolist())
            self.insert_data("road", chunk[["id", "pref_no", "course_no", "course_name", "dir_name", "low_kp", 
                                            "low_latitude", "low_longitude", "low_altitude", "low_spot_name", 
                                            "low_cityname_code", "up_kp", "up_latitude", "up_longitude", 
                                            "up_altitude", "up_spot_name", "up_cityname_code"]].values.tolist())
            self.insert_data("regulation", chunk.rename(columns={"offer_date": "time"})[["id", "time", "event_no", 
                                                                                         "event_seq", "regulation", 
                                                                                         "link_distance", 
                                                                                         "reason"]].values.tolist())

    def insert_1hour(self, file_path_1hour):
        """
        Creates and inserts data into 1-hour interval tables.

        Args:
            file_path_1hour (str): Path to the 1-hour interval CSV data file.
        """
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
        self.cursor.execute("SELECT create_distributed_table('congestion_1hour', 'id')")

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
        self.cursor.execute("SELECT create_distributed_table('road_1hour', 'id')")
        self.conn.commit()

        # Insert data
        chunk_size = 10000
        for chunk in pd.read_csv(file_path_1hour, chunksize=chunk_size):
            if 'id' not in chunk.columns:
                chunk['id'] = range(1, len(chunk) + 1)

            self.insert_data("congestion_1hour", chunk[["id", "time", "allCount", "lightCongestion", 
                                                        "heavyCongestion", "averageLength", "maxLength", 
                                                        "congestionTime", "congestionAmount", "linkLength"]].values.tolist())
            self.insert_data("road_1hour", chunk.rename(columns={"ColumnInCSV": "roadName"})[[
                "id", "roadName", "direction", "dwLocation", "dwLatitude", "dwLongitude", 
                "upLocation", "upLatitude", "upLongitude"]].values.tolist())

    def insert_data(self, table_name, data):
        """
        Inserts data into a distributed table while handling conflicts.

        Args:
            table_name (str): The name of the table to insert data into.
            data (list): A list of data rows to insert.
        """
        temp_table = f"temp_{table_name}"
        temp_file = f"temp_{table_name}.csv"
    
        try:
            # Step 1: Create a temporary table
            self.cursor.execute(f'''
                CREATE TEMP TABLE {temp_table} (LIKE {table_name} INCLUDING DEFAULTS)
            ''')
    
            # Step 2: Write data to a temporary CSV file
            with open(temp_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(data)
    
            # Step 3: Load data into the temporary table
            with open(temp_file, 'r') as f:
                self.cursor.copy_expert(f"COPY {temp_table} FROM STDIN WITH CSV", f)
    
            # Step 4: Merge data into the main table with conflict handling
            self.cursor.execute(f'''
                INSERT INTO {table_name}
                SELECT * FROM {temp_table}
                ON CONFLICT (id) DO NOTHING
            ''')

            self.conn.commit()
        finally:
            # Clean up: Drop the temporary table and delete the temporary file
            self.cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
            if os.path.exists(temp_file):
                os.remove(temp_file)

    def close(self):
        """
        Closes the database connection and cursor.
        """
        self.cursor.close()
        self.conn.close()
