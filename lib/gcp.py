# This file is part of RESTerville, a Workflow Automation toolkit.
# Copyright (C) 2024  GEOACE

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

# You can contact the developer via email or using the contact form provided at https://geoace.net

import logging
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import storage

# Configure logging
logging.basicConfig(level=logging.INFO)

def get_gcs_bucket(bucket_name) -> storage.Bucket:
    """Initialize GCS client and get bucket reference based on the environment."""
    logging.basicConfig(level=logging.INFO)
    credentials = None

    try:
        credentials, project = default()
        logging.info(f"Default credentials loaded successfully for project: {project}.")
    except DefaultCredentialsError as e:
        logging.error("No credentials provided and default auth failed: %s", e)
        raise

    try:
        # Initialize the Google Cloud Storage client
        client = storage.Client(credentials=credentials)
        logging.info("Google Cloud Storage client initialized successfully.")
        
        bucket = client.bucket(bucket_name)
        logging.info(f"Bucket reference for '{bucket_name}' obtained successfully.")
        
        # Check if the bucket exists and has appropriate permissions
        if not bucket.exists():
            logging.error(f"Bucket '{bucket_name}' does not exist.")
            raise Exception(f"Bucket '{bucket_name}' does not exist.")
        else:
            logging.info(f"Bucket '{bucket_name}' exists and is accessible.")
    except Exception as e:
        logging.error("An unexpected error occurred: %s", e, exc_info=True)
        raise

    return bucket