# Use the official Python image from the Docker Hub
FROM python:3.9

# Set the working directory
WORKDIR .

# Copy the source code and .proto files
COPY . .

RUN pip install --no-cache-dir -r requirements.txt
# Expose port 5440 for gRPC server
EXPOSE 5440
# Copy the datasets to the appropriate directory
COPY partitions/part_0.csv /partitions/part_0.csv
COPY partitions/part_1.csv /partitions/part_1.csv
COPY inputs /inputs

# Command to run the server by default
CMD ["python3", "-u", "server.py"]

