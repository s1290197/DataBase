import pandas as pd
from pymongo import MongoClient

class MongoDB:
    """
    A class for managing MongoDB database operations, including inserting data at 5-minute and 1-hour intervals,
    deleting collections or specific rows, and handling batch inserts.

    Attributes:
        client (MongoClient): MongoDB client instance.
        db (Database): Database object for performing operations.
    """

    def __init__(self):
        """
        Initializes the MongoDB client and selects the database.
        """
        self.client = MongoClient("mongodb://163.143.165.154:27018/")
        self.db = self.client["congestion"]

    def delete(self, operation):
        """
        Deletes specified collections or rows from the database based on the operation type.

        Args:
            operation (str): Specifies which collections or rows to delete.
                Options: "5min", "1hour", "geography", "all", or "row:<collection_name>:<id>".
        """
        if operation == "5min" or operation == "all":
            self.db.drop_collection("congestion")
            self.db.drop_collection("road")
            self.db.drop_collection("regulation")
            self.db.drop_collection("geography")

        if operation == "1hour" or operation == "all":
            self.db.drop_collection("congestion_1hour")
            self.db.drop_collection("road_1hour")

        if operation.startswith("row:"):
            collection_name, document_id = operation.split(":")[1], operation.split(":")[2]
            self.db[collection_name].delete_one({"_id": document_id})

    def batch_insert(self, collection_name, data, batch_size=100):
        """
        Inserts data into MongoDB in batches.

        Args:
            collection_name (str): Name of the collection to insert data into.
            data (list): List of documents to be inserted.
            batch_size (int): Number of documents to insert in each batch (default is 100).
        """
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            self.db[collection_name].insert_many(batch)

    def insert_5min(self, file_path_5min):
        """
        Inserts 5-minute interval data into MongoDB collections.

        Args:
            file_path_5min (str): Path to the CSV file containing 5-minute interval data.
        """
        chunk_size = 100
        for chunk in pd.read_csv(file_path_5min, sep="\t", low_memory=False, chunksize=chunk_size):
            chunk.rename(columns={'Unnamed: 0': 'id'}, inplace=True)

            # Insert congestion data
            congestion_data = chunk[["id", "offer_date", "offer_day", "offer_hour", "event_no",
                                     "congestion_degree", "congestion_length"]].to_dict(orient="records")
            self.batch_insert("congestion", congestion_data)

            # Insert road data
            road_data = chunk[["id", "pref_no", "course_no", "course_name", "dir_name", "low_kp",
                               "low_latitude", "low_longitude", "low_altitude", "low_spot_name",
                               "low_cityname_code", "up_kp", "up_latitude", "up_longitude", "up_altitude",
                               "up_spot_name", "up_cityname_code"]]
            road_data = road_data.rename(columns={
                "course_name": "roadname",
                "dir_name": "direction",
                "low_spot_name": "dwlocation",
                "low_latitude": "dwlatitude",
                "low_longitude": "dwlongitude",
                "up_spot_name": "uplocation",
                "up_latitude": "uplatitude",
                "up_longitude": "uplongitude"
            }).to_dict(orient="records")
            self.batch_insert("road", road_data)

            # Insert regulation data
            regulation_data = chunk[["id", "offer_date", "event_no", "event_seq", "regulation",
                                     "link_distance", "reason"]]
            regulation_data = regulation_data.rename(columns={"offer_date": "time"}).to_dict(orient="records")
            self.batch_insert("regulation", regulation_data)

    def insert_1hour(self, file_path_1hour):
        """
        Inserts 1-hour interval data into MongoDB collections.

        Args:
            file_path_1hour (str): Path to the CSV file containing 1-hour interval data.
        """
        chunk_size = 100
        for chunk in pd.read_csv(file_path_1hour, chunksize=chunk_size):
            # Ensure `id` column exists
            if 'id' not in chunk.columns:
                chunk['id'] = range(1, len(chunk) + 1)

            # Ensure all required columns exist
            required_columns = ["id", "time", "allCount", "lightCongestion", "heavyCongestion",
                                "averageLength", "maxLength", "congestionTime", "congestionAmount", "linkLength"]
            for col in required_columns:
                if col not in chunk.columns:
                    chunk[col] = None

            # Insert congestion_1hour data
            congestion_1hour_data = chunk[required_columns].to_dict(orient="records")
            self.batch_insert("congestion_1hour", congestion_1hour_data)

            # Insert road data
            road_data = chunk[["id", "roadName", "direction", "dwLocation", "dwLatitude", "dwLongitude",
                               "upLocation", "upLatitude", "upLongitude"]]
            road_data = road_data.rename(columns={
                "roadName": "roadname",
                "dwLocation": "dwlocation",
                "dwLatitude": "dwlatitude",
                "dwLongitude": "dwlongitude",
                "upLocation": "uplocation",
                "upLatitude": "uplatitude",
                "upLongitude": "uplongitude"
            }).to_dict(orient="records")
            self.batch_insert("road_1hour", road_data)

    def close(self):
        """
        Closes the MongoDB connection.
        """
        self.client.close()
