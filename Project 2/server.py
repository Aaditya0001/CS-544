import sys
import csv
import grpc
from concurrent import futures
import socket
import matchdb_pb2_grpc  # Import generated gRPC code
import matchdb_pb2       # Import message types

class MatchCountServicer(matchdb_pb2_grpc.MatchCountServicer):
    def __init__(self, data):
        self.data = data

    def GetMatchCount(self, request, context):
        country = request.country
        winning_team = request.winning_team

        count = 0
        for row in self.data:
            if (not country or row['country'] == country) and (not winning_team or row['winning_team'] == winning_team):
                count += 1

        return matchdb_pb2.GetMatchCountResp(num_matches=count)

def load_csv(file_path):
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        return list(reader)

def get_partition_data():
    # Get the hostname and IP of the current container
    container_ip = socket.gethostbyname(socket.gethostname())
    
    # Get the IPs for wins-server-1 and wins-server-2
    server_1_ip = socket.gethostbyname("wins-server-1")
    server_2_ip = socket.gethostbyname("wins-server-2")

    # If running inside server 1 container, serve partition 0
    if container_ip == server_1_ip:
        return "/partitions/part_0.csv", 5440  # Update with actual path to partition 0
    # If running inside server 2 container, serve partition 1
    elif container_ip == server_2_ip:
        return "/partitions/part_1.csv", 5440  # Update with actual path to partition 1

    # Default case (should not occur if container setup is correct)
    return None, 5440

def serve(file_path, port):
    # Load the CSV data
    data = load_csv(file_path)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    match_count_servicer = MatchCountServicer(data)
    matchdb_pb2_grpc.add_MatchCountServicer_to_server(match_count_servicer, server)
    
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f'Serving on port {port}...')
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("Server stopping...")

if __name__ == '__main__':
    # Check if arguments are provided
    if len(sys.argv) > 1:
        csv_file_path = sys.argv[1]
        port = int(sys.argv[2])
    else:
        # Determine file and port from container name
        csv_file_path, port = get_partition_data()

    print(f"Using data file: {csv_file_path}")
    print(f"Server starting on port: {port}")

    serve(csv_file_path, port)

