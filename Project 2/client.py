import csv
import grpc
from collections import OrderedDict
from matchdb_pb2 import GetMatchCountReq
from matchdb_pb2_grpc import MatchCountStub

def simple_hash(country):
    out = 0
    for c in country:
        out += (out << 2) - out + ord(c)
    return out

class LRUCache:
    def __init__(self, size):
        self.cache = OrderedDict()
        self.size = size

    def get(self, key):
        if key not in self.cache:
            return None
        else:
            # Move the accessed item to the end (most recently used)
            self.cache.move_to_end(key)
            return self.cache[key]

    def put(self, key, value):
        if key in self.cache:
            # Move the accessed item to the end
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.size:
            # Remove the first item (least recently used)
            self.cache.popitem(last=False)

def main(server_0, server_1, input_file):
    # Create a gRPC channel for both servers
    channel_0 = grpc.insecure_channel(server_0)
    channel_1 = grpc.insecure_channel(server_1)
    stub_0 = MatchCountStub(channel_0)
    stub_1 = MatchCountStub(channel_1)

    # Initialize LRU Cache
    lru_cache = LRUCache(size=10)

    with open(input_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            winning_team = row['winning_team']
            country = row['country']
            cache_key = (winning_team, country)

            # Check the cache first
            cached_result = lru_cache.get(cache_key)
            if cached_result is not None:
                print(f"{cached_result}*")  # Print with star for cache hit
                continue  # Skip to the next row if in cache

            # Determine which server(s) to query
            count = 0
            if country:
                # Determine which partition the country belongs to
                partition = simple_hash(country) % 2
                if partition == 0:
                    response = stub_0.GetMatchCount(GetMatchCountReq(country=country, winning_team=winning_team))
                    count += response.num_matches
                else:
                    response = stub_1.GetMatchCount(GetMatchCountReq(country=country, winning_team=winning_team))
                    count += response.num_matches
            else:
                # No country specified, query both servers
                response_0 = stub_0.GetMatchCount(GetMatchCountReq(country="", winning_team=winning_team))
                response_1 = stub_1.GetMatchCount(GetMatchCountReq(country="", winning_team=winning_team))
                count += response_0.num_matches + response_1.num_matches

            # Cache the result
            lru_cache.put(cache_key, count)
            print(count)  # Print the count for this query

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        print("Usage: python3 client.py <SERVER_0> <SERVER_1> <INPUT_FILE>")
        sys.exit(1)
    
    server_0 = sys.argv[1]
    server_1 = sys.argv[2]
    input_file = sys.argv[3]
    
    main(server_0, server_1, input_file)

