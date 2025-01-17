#!/bin/bash

# Download the ZIP file
echo "Downloading ZIP file..."
wget -q https://pages.cs.wisc.edu/~harter/cs544/data/hdma-wi-2021.zip

# Extract the contents
echo "Extracting ZIP file..."
unzip -o hdma-wi-2021.zip

# Count lines containing "Multifamily"
echo "Counting lines containing 'Multifamily'..."
count=$(grep -c "Multifamily" *.csv)

# Output the count
echo "Number of lines containing 'Multifamily': $count"
