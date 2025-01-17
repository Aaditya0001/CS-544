# Use a lightweight base image
FROM ubuntu:20.04

# Set the working directory
WORKDIR /app

# Install necessary packages
RUN apt-get update && \
    apt-get install -y wget unzip grep && \
    rm -rf /var/lib/apt/lists/*

# Copy the count.sh script into the container
COPY count.sh .

# Make the count.sh script executable
RUN chmod +x count.sh

# Default command to run the count.sh script
CMD ["./count.sh"]
