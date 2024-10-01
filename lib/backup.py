
# This file is part of RESTerville.

# RESTerville is licensed under the GNU General Public License v3.0.
# See the LICENSE file for more information.

# This file uses the ArcGIS API for Python, which is licensed under the Apache License 2.0.
# See https://github.com/Esri/arcgis-python-api/blob/master/LICENSE for more information.

# This file incorporates work that is licensed under the Creative Commons Attribution 4.0 International License.
# See https://creativecommons.org/licenses/by/4.0/ for more information.

# Attribution:
# This file uses code or data provided by Google, available at https://cloud.google.com/python/docs/reference.

import os
import sys
import datetime as dt
import argparse
import logging
from arcgis.gis import GIS
import time
import re
from gcp import get_gcs_bucket

# Configure logging for this script
logging.basicConfig(
    level=logging.INFO,  # Adjust to the appropriate level
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Apply the flush handler to ensure immediate output
class FlushHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()

logging.getLogger().handlers = [FlushHandler(sys.stdout)]

# Define retry parameters
MAX_UPLOAD_RETRIES = 3
RETRY_DELAY_SECONDS = 5
DEFAULT_TIMEOUT = (10, 3600)  # 30-minute read timeout

def sanitize_name(name):
    """Replace all special characters in a name with underscores."""
    return re.sub(r'[^A-Za-z0-9_]', '_', name)

def parse_args():
    parser = argparse.ArgumentParser(description='Backup GIS items and manage archives.')
    parser.add_argument('--max_items', type=int, help='Maximum number of items to process', default=100)
    parser.add_argument('--duration', type=int, help='Duration in days to keep stored backup files', required=False)
    parser.add_argument('--remove_archives', choices=['yes', 'no'], default='no', help='Whether to remove old archives (yes/no)')
    parser.add_argument('--bucket_name', required=True, help='Name of the Google Cloud Storage bucket')
    parser.add_argument('--usernames', required=True, help='Comma-separated list of usernames to back up')
    return parser.parse_args()

def upload_with_retry(blob, local_path, retries=MAX_UPLOAD_RETRIES):
    """Upload a file to Google Cloud Storage with retries."""
    for attempt in range(1, retries + 1):
        try:
            blob.upload_from_filename(local_path, timeout=DEFAULT_TIMEOUT[1])  # Use the read timeout
            logging.info(f"Successfully uploaded {blob.name}")
            return True
        except Exception as e:
            logging.error(f"Attempt {attempt}/{retries} failed with error: {e}", exc_info=True)
            if attempt < retries:
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                logging.error(f"Upload failed after {retries} attempts.")
                return False

def list_existing_files(bucket):
    """List all the files in the bucket and store them in a dictionary for easy lookup."""
    blobs = bucket.list_blobs()
    existing_files = {}

    for blob in blobs:
        filename = blob.name
        if filename.endswith('.gdb.zip'):
            base_name = filename[:-8]  # Remove `.gdb.zip`
        elif filename.endswith('.zip'):
            base_name = filename[:-4]  # Remove `.zip`
        else:
            continue  # Skip unknown extensions

        existing_files[base_name] = filename

    return existing_files

def get_last_modified_date(bucket, base_name):
    """Get the last modified date from the file name in the bucket."""
    blobs = bucket.list_blobs()
    latest_date = None

    for blob in blobs:
        filename = blob.name
        if base_name in filename:
            name_parts = filename.split('_')
            date_str = '_'.join(name_parts[-3:]).replace('.gdb.zip', '')
            try:
                file_date = dt.datetime.strptime(date_str, '%d_%b_%Y')
                if not latest_date or file_date > latest_date:
                    latest_date = file_date
            except ValueError:
                logging.error(f"Error parsing date from filename {filename}", exc_info=True)

    return latest_date

def download_as_fgdb(item_list, bucket, max_items):
    today_date = dt.datetime.now().strftime("%d_%b_%Y")
    added_files = []
    skipped_files = []

    # Retrieve the existing files in the bucket
    existing_files = list_existing_files(bucket)

    for count, item in enumerate(item_list):
        if count >= max_items:
            logging.info(f"Reached the maximum limit of {max_items} items to process.")
            break

        sanitized_name = sanitize_name(item.title)
        base_name = f"{sanitized_name}_{today_date}"

        # Determine the last modified date of the existing backup
        last_backup_date = get_last_modified_date(bucket, sanitized_name)

        # Convert item.modified to datetime
        item_modified_date = dt.datetime.fromtimestamp(item.modified / 1000)

        # Skip the item if it hasn't been modified since the last backup
        if last_backup_date and item_modified_date <= last_backup_date:
            logging.info(f"Skipping {item.title}, not modified since last backup.")
            skipped_files.append(item.title)
            continue

        try:
            logging.info(f"Downloading {item.title}")
            result = item.export(f"{sanitized_name}_{today_date}", "File Geodatabase")
            local_path = result.download()

            blob = bucket.blob(f"{base_name}.gdb.zip")

            if upload_with_retry(blob, local_path):
                added_files.append(item.title)
                os.remove(local_path)
                result.delete()
            else:
                skipped_files.append(item.title)

        except Exception as e:
            logging.error(f"An error occurred downloading {item.title}: {e}", exc_info=True)
            skipped_files.append(item.title)

    logging.info("The function has completed")
    return added_files, skipped_files

def delete_old_archives(bucket, duration, added_files):
    now = dt.datetime.now()
    cutoff_date = now - dt.timedelta(days=duration)
    blobs = bucket.list_blobs()

    for blob in blobs:
        filename = blob.name
        # Determine the date format based on the file extension
        if filename.endswith('.gdb.zip'):
            base_name = filename[:-8]  # Removes `.gdb.zip`
        elif filename.endswith('.zip'):
            base_name = filename[:-4]  # Removes `.zip`
        else:
            logging.error(f"Unknown file extension for {filename}. Skipping.")
            continue

        name_parts = base_name.split('_')
        layer_name = '_'.join(name_parts[:-3])
        date_str = '_'.join(name_parts[-3:])

        try:
            file_date = dt.datetime.strptime(date_str, '%d_%b_%Y')
            if file_date < cutoff_date and (layer_name not in added_files or file_date < cutoff_date):
                blob.delete()
                logging.info(f"Deleted old archive: {filename}")
            else:
                logging.info(f"Kept archive {filename}: Either most recent or does not meet cutoff date.")
        except ValueError as e:
            logging.error(f"Error parsing date from filename {filename}: {e}", exc_info=True)

def main():
    args = parse_args()
    if args.remove_archives == 'yes' and args.duration is None:
        logging.error("Duration must be specified if remove_archives is set to 'yes'")
        sys.exit(1)

    bucket = get_gcs_bucket(args.bucket_name)

    portal_url = os.getenv('ARCGIS_PORTAL_URL')
    user = os.getenv('ARCGIS_USER')
    password = os.getenv('ARCGIS_PASSWORD')
    gis = GIS(portal_url, user, password)

    # Split the usernames from the argument
    usernames = [name.strip() for name in args.usernames.split(',')]

    # Collect items from all specified users
    items = []
    for username in usernames:
        try:
            logging.info(f"Searching for items owned by {username}")
            user_items = gis.content.search(
                query=f"type:Feature Service AND owner:{username}",
                max_items=args.max_items,
                sort_field='modified',
                sort_order='desc'
            )
            items.extend(user_items)
        except Exception as e:
            logging.error(f"An error occurred while searching for items owned by {username}: {e}", exc_info=True)

    # Perform the download and backup operations
    added_files, skipped_files = download_as_fgdb(items, bucket, args.max_items)

    if args.remove_archives == 'yes':
        delete_old_archives(bucket, args.duration, [sanitize_name(item) for item in added_files])

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)