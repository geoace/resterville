
# This file is part of RESTerville.

# RESTerville is licensed under the GNU General Public License v3.0.
# See the LICENSE file for more information.

# This file uses the ArcGIS API for Python, which is licensed under the Apache License 2.0.
# See https://github.com/Esri/arcgis-python-api/blob/master/LICENSE for more information.

# This file incorporates work that is licensed under the Creative Commons Attribution 4.0 International License.
# See https://creativecommons.org/licenses/by/4.0/ for more information.

# Attribution:
# This file uses code or data provided by Google, available at https://cloud.google.com/python/docs/reference.

import argparse
import datetime as dt
import logging
import os
import re
import sys
import time

from arcgis.gis import GIS

from lib.gcp import get_gcs_bucket

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
    parser = argparse.ArgumentParser(
        description='Backup GIS items and manage archives.')
    parser.add_argument('--max_items', type=int,
                        help='Maximum number of items to process', default=100)
    parser.add_argument('--duration', type=int,
                        help='Duration in days to keep stored backup files', required=False)
    parser.add_argument('--remove_archives', choices=[
                        'yes', 'no'], default='no', help='Whether to remove old archives (yes/no)')
    parser.add_argument('--bucket_name', required=True,
                        help='Name of the Google Cloud Storage bucket')
    parser.add_argument('--usernames', required=True,
                        help='Comma-separated list of usernames to back up')
    return parser.parse_args()


def upload_with_retry(blob, local_path, retries=MAX_UPLOAD_RETRIES):
    """Upload a file to Google Cloud Storage with retries."""
    for attempt in range(1, retries + 1):
        try:
            blob.upload_from_filename(
                local_path, timeout=DEFAULT_TIMEOUT[1])  # Use the read timeout
            logging.info(f"Successfully uploaded {blob.name}")
            return True
        except Exception as e:
            logging.error(
                f"Attempt {attempt}/{retries} failed with error: {e}", exc_info=True)
            if attempt < retries:
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                logging.error(f"Upload failed after {retries} attempts.")
                return False


def _parse_backup_name(filename):
    """Split '<layer>_<DD>_<Mon>_<YYYY>.gdb.zip' into (layer, date). Returns None if it doesn't match."""
    if filename.endswith('.gdb.zip'):
        base_name = filename[:-8]
    elif filename.endswith('.zip'):
        base_name = filename[:-4]
    else:
        return None
    name_parts = base_name.split('_')
    if len(name_parts) < 4:
        return None
    try:
        file_date = dt.datetime.strptime('_'.join(name_parts[-3:]), '%d_%b_%Y')
    except ValueError:
        return None
    return '_'.join(name_parts[:-3]), file_date


def index_backups(bucket):
    """List the bucket once and group backup blobs by layer name: {layer: [(date, blob), ...]}."""
    backups = {}
    for blob in bucket.list_blobs():
        parsed = _parse_backup_name(blob.name)
        if not parsed:
            continue
        layer_name, file_date = parsed
        backups.setdefault(layer_name, []).append((file_date, blob))
    return backups


def download_as_fgdb(item_list, bucket, max_items):
    today_date = dt.datetime.now().strftime("%d_%b_%Y")
    added_files = []
    skipped_files = []

    # One bucket listing for the whole run. The old code listed the bucket once
    # per item and matched backups by substring, so "Parcel" also matched
    # "Parcel_Lines" and could skip an item that had changed.
    backups = index_backups(bucket)

    for count, item in enumerate(item_list):
        if count >= max_items:
            logging.info(
                f"Reached the maximum limit of {max_items} items to process.")
            break

        sanitized_name = sanitize_name(item.title)
        base_name = f"{sanitized_name}_{today_date}"

        existing = backups.get(sanitized_name, [])
        last_backup_date = max((d for d, _ in existing), default=None)

        item_modified_date = dt.datetime.fromtimestamp(item.modified / 1000)

        if last_backup_date and item_modified_date <= last_backup_date:
            logging.info(
                f"Skipping {item.title}, not modified since last backup.")
            skipped_files.append(item.title)
            continue

        result = None
        try:
            logging.info(f"Downloading {item.title}")
            result = item.export(base_name, "File Geodatabase")
            local_path = result.download()

            blob = bucket.blob(f"{base_name}.gdb.zip")

            if upload_with_retry(blob, local_path):
                added_files.append(item.title)
            else:
                skipped_files.append(item.title)
            os.remove(local_path)

        except Exception as e:
            logging.error(
                f"An error occurred downloading {item.title}: {e}", exc_info=True)
            skipped_files.append(item.title)
        finally:
            # Remove the temporary export item from the portal whether or not the upload worked.
            if result is not None:
                try:
                    result.delete()
                except Exception as e:  # pylint: disable=broad-exception-caught
                    logging.warning(f"Could not delete export item for {item.title}: {e}")

    logging.info("The function has completed")
    return added_files, skipped_files


def delete_old_archives(bucket, duration):
    """Delete backups older than `duration` days, always keeping the newest backup of each layer.

    Unchanged items are skipped by download_as_fgdb, so their newest backup can
    be older than the cutoff. The old code deleted it anyway, which left those
    layers with no backup at all once `duration` days had passed.
    """
    cutoff_date = dt.datetime.now() - dt.timedelta(days=duration)

    for layer_name, entries in index_backups(bucket).items():
        entries.sort(key=lambda e: e[0], reverse=True)
        newest, older = entries[0], entries[1:]
        logging.info(f"Keeping newest backup of {layer_name}: {newest[1].name}")
        for file_date, blob in older:
            if file_date < cutoff_date:
                blob.delete()
                logging.info(f"Deleted old archive: {blob.name}")
            else:
                logging.info(f"Kept archive {blob.name}: within {duration} days.")


def main():
    args = parse_args()
    if args.remove_archives == 'yes' and args.duration is None:
        logging.error(
            "Duration must be specified if remove_archives is set to 'yes'")
        sys.exit(1)

    bucket = get_gcs_bucket(args.bucket_name)

    portal_url = os.getenv('ARCGIS_PORTAL_URL')
    user = os.getenv('ARCGIS_USER')
    password = os.getenv('ARCGIS_PASSWORD')
    gis = GIS(portal_url, user, password)

    # Split the usernames from the argument
    usernames = [name.strip() for name in args.usernames.split(',') if name.strip()]
    for username in usernames:
        if not re.match(r'^[A-Za-z0-9_.@-]+$', username):
            logging.error(f"Invalid username: {username!r}")
            sys.exit(1)

    # Collect items from all specified users
    items = []
    for username in usernames:
        try:
            logging.info(f"Searching for items owned by {username}")
            user_items = gis.content.search(
                query=f'type:"Feature Service" AND owner:"{username}"',
                max_items=args.max_items,
                sort_field='modified',
                sort_order='desc'
            )
            items.extend(user_items)
        except Exception as e:
            logging.error(
                f"An error occurred while searching for items owned by {username}: {e}", exc_info=True)

    # Perform the download and backup operations
    added_files, skipped_files = download_as_fgdb(
        items, bucket, args.max_items)

    logging.info(f"Backed up {len(added_files)} items, skipped {len(skipped_files)}.")

    if args.remove_archives == 'yes':
        delete_old_archives(bucket, args.duration)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)
