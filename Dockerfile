# Use an official GDAL image
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.8.5

# Set the working directory in the container
WORKDIR /app

# Install Python, pip, and other necessary packages, and setup virtual environment
RUN apt-get update && \
    apt-get install -y python3 python3-pip libkrb5-dev wget gnupg lsb-release python3-venv && \
    python3 -m venv /venv && \
    rm -rf /var/lib/apt/lists/*

# Upgrade pip and install dependencies using the virtual environment
COPY requirements.txt /app/requirements.txt
RUN . /venv/bin/activate && \
    pip install --upgrade pip && \
    pip install -r /app/requirements.txt && \ 
    mkdir -p /app/env

# Copy the rest of the application and prepare the entrypoint script
COPY . /app
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Expose the port the app runs on and set the entrypoint and command
EXPOSE 8080
ENTRYPOINT ["/entrypoint.sh"]
CMD ["/venv/bin/python", "app.py"]
