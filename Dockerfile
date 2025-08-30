# Use an official lightweight Python image as the base
FROM python:3.12-slim

# Install system dependencies required for lxml
RUN apt-get update && apt-get install -y --no-install-recommends libxml2-dev libxslt-dev && rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install the dependencies specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script file into the container
COPY nasdaq.py .

# Command to run the script when the container starts
CMD ["python", "nasdaq.py"]