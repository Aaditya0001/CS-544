# Use the official Python slim image as a base
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app
COPY server.py .
COPY table.proto .
COPY requirements.txt .
# Copy the server.py and any other necessary files (like requirements.txt)

# Install any necessary packages
RUN pip install --no-cache-dir -r requirements.txt

RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. table.proto

# Expose the port that the server will run on
EXPOSE 5440

# Command to run the server
CMD ["python", "server.py"]

