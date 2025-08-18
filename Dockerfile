# Start with the official Python image
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Upgrade pip and install psycopg2-binary
RUN pip install --upgrade pip
RUN pip install psycopg2-binary

# Install the PostgreSQL client to use 'psql'
RUN apt-get update && apt-get install -y postgresql-client

# Copy your requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your bot's code
COPY . .

# Command to run your bot
CMD ["python", "main.py"]