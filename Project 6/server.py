import grpc
from concurrent import futures
import station_pb2
import station_pb2_grpc
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring
from cassandra.query import ConsistencyLevel
from cassandra import Unavailable
from cassandra.cluster import NoHostAvailable

class StationService(station_pb2_grpc.StationServicer):
    def __init__(self):
        # Connect to the Cassandra cluster
        self.cluster = Cluster(['p6-db-1', 'p6-db-2', 'p6-db-3'])
        self.session = self.cluster.connect()

        # Create a Spark session
        self.spark = SparkSession.builder.appName("p6").getOrCreate()

        # TODO: create schema for weather data; 
        self.setup_cassandra()
        # TODO: load station data from ghcnd-stations.txt; 
        stations_df = self.spark.read.text("ghcnd-stations.txt")
        wisconsin_stations = stations_df.collect()

        # Loop through the rows, extract id and name, and insert into Cassandra
        for row in wisconsin_stations:
            state = row.value[38:40].strip()  # Extract state code (characters 38 to 40)
            
            # Only process Wisconsin stations (state code "WI")
            if state == 'WI':
                station_id = row.value[0:11].strip()  # Extract station ID (first 11 characters)
                station_name = row.value[41:72].strip()  # Extract station name (characters 41 to 72)
                
                # Escape any single quotes in the name for SQL insertion
                station_name = station_name.replace("'", "''")
                
                # Prepare the query to insert the data into the Cassandra table
                query = f"INSERT INTO weather.stations (id, name) VALUES ('{station_id}', '{station_name}')"
                
                # Execute the query to insert the station data
                self.session.execute(query)

        # ============ Server Stated Successfully =============
        print("Server started") # Don't delete this line!

    def setup_cassandra(self):
        # Drop the keyspace if it already exists
        self.session.execute("DROP KEYSPACE IF EXISTS weather")

        # Create the weather keyspace with replication factor of 3
        self.session.execute("""
            CREATE KEYSPACE weather WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3}
        """)

        # Use the new weather keyspace
        self.session.set_keyspace('weather')

        # Create the station_record type (contains tmin and tmax)
        self.session.execute("""
            CREATE TYPE IF NOT EXISTS station_record (
                tmin INT,
                tmax INT
            )
        """)

        # Create the stations table
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS stations (
                id TEXT,
                date DATE,
                name TEXT STATIC,
                record station_record,
                PRIMARY KEY (id, date)
            ) WITH CLUSTERING ORDER BY (date ASC)
        """)

    def StationSchema(self, request, context):
        # Describe the stations table and extract the schema
        result = self.session.execute("DESCRIBE TABLE weather.stations")
        schema = result[0].create_statement  # The schema is in the first column
        return station_pb2.StationSchemaReply(schema=schema, error="")

    def StationName(self, request, context):
        station_id = request.station
        
        # Query Cassandra for the station name
        query = "SELECT name FROM stations WHERE id = %s"
        result = self.session.execute(query, (station_id,))
        
        # If we have a result, return the station name
        if result:
            station_name = result[0].name
            return station_pb2.StationNameReply(name=station_name, error="")
        else:
            # Return error if station ID not found
            return station_pb2.StationNameReply(name="", error="Station ID not found")

    def RecordTemps(self, request, context):
        try:
        # Prepare the statement (no explicit write consistency needed)
            prepared = self.session.prepare("""
                INSERT INTO stations (id, date, record)
                VALUES (?, ?, {tmin: ?, tmax: ?})
                """)

        # Execute the prepared statement
            bound = prepared.bind((request.station, request.date, request.tmin, request.tmax))
            self.session.execute(bound)

        # Return a successful response
            return station_pb2.RecordTempsReply(error="")
        except (Unavailable, NoHostAvailable):
        # Handle Cassandra-specific availability errors
            return station_pb2.RecordTempsReply(error="unavailable")
        except Exception as e:
        # Handle all other exceptions
            return station_pb2.RecordTempsReply(error=f"Unexpected error: {str(e)}")


    def StationMax(self, request, context):
        try:
        # Prepare the statement with a consistency level of TWO for strong reads
            prepared = self.session.prepare("""
                SELECT MAX(record.tmax) AS max_tmax
                FROM stations
                WHERE id = ?
            """)
            prepared.consistency_level = ConsistencyLevel.THREE

        # Execute the prepared statement
            bound = prepared.bind((request.station,))
            result = self.session.execute(bound)

        # Extract max_tmax from the result
            max_tmax = result.one().max_tmax if result.one() else None

            if max_tmax is not None:
            # Return the maximum tmax
                return station_pb2.StationMaxReply(tmax=max_tmax, error="")
            else:
            # Return an error if no data is found
                return station_pb2.StationMaxReply(tmax=-1, error="No data found for the given station")
        except (Unavailable, NoHostAvailable):
        # Handle Cassandra-specific availability errors
            return station_pb2.StationMaxReply(tmax=-1, error="unavailable")
        except Exception as e:
        # Handle all other exceptions
            return station_pb2.StationMaxReply(tmax=-1, error=f"Unexpected error: {str(e)}")


        

        



def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=9),
        options=[("grpc.so_reuseport", 0)],
    )
    station_pb2_grpc.add_StationServicer_to_server(StationService(), server)
    server.add_insecure_port('0.0.0.0:5440')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
