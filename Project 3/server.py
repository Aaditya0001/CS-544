import grpc
from concurrent import futures
import logging
import os
import pandas as pd
import pyarrow.parquet as pq
import time
import threading
import table_pb2_grpc
from table_pb2 import UploadReq, UploadResponse, ColSumReq, ColSumResponse

# Global lock for thread-safe access to shared data
lock = threading.Lock()

class TableService(table_pb2_grpc.TableServicer):
    def __init__(self):
        self.file_paths = []  # List to keep track of uploaded file paths

    def Upload(self, request, context):
        logging.info("Received Upload request")
        
        # Get the CSV data as bytes
        csv_data = request.csv_data

        # Generate a unique filename based on the current timestamp
        timestamp = int(time.time() * 1000)
        unique_csv_filename = f"uploads/data_{timestamp}.csv"
        unique_parquet_filename = f"uploads/data_{timestamp}.parquet"

        # Ensure the uploads directory exists
        os.makedirs("uploads", exist_ok=True)

        # Write byte data to the unique CSV file
        with open(unique_csv_filename, 'wb') as csv_file:
            csv_file.write(csv_data)

        # Convert CSV to Parquet format
        df = pd.read_csv(unique_csv_filename)
        df.to_parquet(unique_parquet_filename, index=False)

        # Acquire the lock before modifying the shared resource
        with lock:
            # Store the file paths
            self.file_paths.append(unique_csv_filename)

        return UploadResponse(success=True, error="")

    def ColSum(self, request, context):
        column_name = request.column
        format_type = request.format

        total_sum = 0

        # Acquire the lock before accessing the shared resource
        with lock:
            files_to_process = self.file_paths.copy()  # Copy the list to process outside the lock

        for file_path in files_to_process:
            try:
                if format_type == "csv":
                    # Read only the specified column from the CSV
                    df = pd.read_csv(file_path, usecols=[column_name])
                    total_sum += df[column_name].sum()
                elif format_type == "parquet":
                    # Read only the specified column from the Parquet file
                    parquet_path = file_path.replace('.csv', '.parquet')
                    table = pq.read_table(parquet_path, columns=[column_name])
                    total_sum += table[column_name].to_pandas().sum()
            except KeyError:
                logging.warning(f"Column '{column_name}' not found in {file_path}.")
            except Exception as e:
                logging.error(f"Error processing {file_path}: {e}")

        return ColSumResponse(total=total_sum, error="")

def serve(port):
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=8),
        options=[("grpc.so_reuseport", 0)]
    )
    table_pb2_grpc.add_TableServicer_to_server(TableService(), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    logging.info(f'Server started on port {port}...')
    server.wait_for_termination()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    serve(5440)

