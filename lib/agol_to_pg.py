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

from typing import Union
import json
import argparse
import tempfile
import os
import traceback
import subprocess
import logging
from io import BytesIO
import requests
from psycopg2 import sql, connect
from psycopg2.extras import execute_values
from esri_to_geojson import esri_to_geojson
from sql import truncate_or_delete_table
from gcp import get_gcs_bucket
from google.cloud.storage import Bucket
from arcgis.gis import GIS

# Configure logging
logging.basicConfig(level=logging.INFO)

def setup_environment(path: str = None) -> None:
    """Set up the environment for PostgreSQL connection."""
    os.environ['PGSERVICEFILE'] = '/app/env/pg_service.conf' if path is None else path

def _get_token() -> str:
    portal_url = os.getenv('ARCGIS_PORTAL_URL')
    user = os.getenv('ARCGIS_USER')
    password = os.getenv('ARCGIS_PASSWORD')
    gis = GIS(portal_url, user, password)

    return gis.session.auth.token

def fetch_data(url: str, start: int, count: int) -> Union[dict, None]:
    """Fetch data from the ArcGIS REST API.

    Args:
        url (str): Url of the ArcGIS REST API.
        start (int): offset to start fetching data from.
        count (int): number of records to fetch.

    Returns:
        Union[dict, None]: The fetched data if found, otherwise None.
    """
    params = {
        'f': 'json',
        'where': '1=1',  # A condition that's always true
        'outFields': '*',  # Fetch all fields
        'resultOffset': start,
        'resultRecordCount': count,
        'token': _get_token()
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch data:", response.text)
        return None

def fetch_attachment_data(url: str, start: int, count: int) -> Union[dict, None]:
    """Fetch attachment data from the ArcGIS REST API.

    Args:
        url (str): Url of the ArcGIS REST API.
        start (int): offset to start fetching data from.
        count (int): number of records to fetch.

    Returns:
        Union[dict, None]: The attachment data if found, otherwise None.
    """
    params = {
        'f': 'json',
        'definitionExpression': '1=1',  # A condition that's always true
        'returnUrl': True,
        'resultOffset': start,
        'resultRecordCount': count,
        'token': _get_token()
    }
    response = requests.get(f'{url}/queryAttachments', params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch data:", response.text)
        return None

def fetch_source_epsg(api_url: str) -> Union[str, None]:
    """Fetch the source EPSG code from the metadata of the ArcGIS REST API.

    Args:
        api_url (str): The URL of the ArcGIS REST API.

    Returns:
        Union[str, None]: The source EPSG code if found, otherwise None.
    """
    metadata_url = f"{api_url}?f=json"
    response = requests.get(metadata_url, params={'token': _get_token()})

    if response.status_code == 200:
        metadata = response.json()
        spatial_ref = metadata.get("extent", {}).get("spatialReference", {})

        if "latestWkid" in spatial_ref:
            return spatial_ref["latestWkid"]
        elif "wkid" in spatial_ref:
            return spatial_ref["wkid"]
        else:
            print("Spatial reference not found in the metadata.")
            return None
    else:
        print(f"Failed to fetch metadata from {metadata_url}: {response.text}")
        return None

def run_ogr2ogr(geojson_file_path: str, service: str, schema: str, table_name: str, geometry_name: str, oid: str, source_epsg: int, target_epsg: int):
    """Run the ogr2ogr command to import GeoJSON data into PostgreSQL.

    Args:
        geojson_file_path (str)
        service (str)
        schema (str)
        table_name (str)
        geometry_name (str)
        oid (str)
        source_epsg (int)
        target_epsg (int)
    """
    with open(geojson_file_path, 'r') as geojson_file:
        geojson_data = json.load(geojson_file)
        if geojson_data["features"]:
            geom_type = geojson_data["features"][0]["geometry"]["type"]

            geom_nlt_mapping = {
                "Point": "POINT",
                "MultiPoint": "MULTIPOINT",
                "LineString": "LINESTRING",
                "MultiLineString": "MULTILINESTRING",
                "Polygon": "POLYGON",
                "MultiPolygon": "MULTIPOLYGON"
            }

            if geom_type == "Polygon":
                geom_nlt = "MULTIPOLYGON"
            elif geom_type == "LineString":
                geom_nlt = "MULTILINESTRING"
            else:
                geom_nlt = geom_nlt_mapping.get(geom_type, "PROMOTE_TO_MULTI")
        else:
            print("No features found in the provided GeoJSON file.")
            return

    command = [
        'ogr2ogr',
        '-progress',
        '--config', 'PG_USE_COPY', 'YES',
        '-f', 'PostgreSQL',
        f"PG:service={service} sslmode=disable active_schema={schema}",
        '-lco', 'DIM=2',
        geojson_file_path,
        '-append',
        '-lco', 'GEOMETRY_NAME=' + geometry_name,
        '-lco', 'FID=' + oid,
        '-nln', schema + '.' + table_name,
        '-a_srs', f'EPSG:{source_epsg}',
        '-nlt', geom_nlt
    ]
    if target_epsg:
        command += ['-t_srs', f'EPSG:{target_epsg}']

    process = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if process.returncode != 0:
        print("ogr2ogr command failed:", process.stderr)
    else:
        print("ogr2ogr command was successful")

def download_features(conn: connect, table_name: str, schema: str, service_name: str, api_url: str, geometry_name: str, oid: str, source_epsg: int, target_epsg: int, batch_size: int) -> None:
    """Download features from the ArcGIS REST API and import them into PostgreSQL.

    Args:
        conn (connect): Database connection.
        table_name (str): Name of the table to import data into.
        schema (str):  Database schema for the table.
        service_name (str): PostgreSQL service name.
        api_url (str): URL of the ArcGIS REST API.
        geometry_name (str): Name of the geometry column.
        oid (str): Name of the object ID column.
        source_epsg (int): Source EPSG code for spatial reference.
        target_epsg (int): Target EPSG code for spatial reference transformation.
        batch_size (int): Number of records to fetch in each batch.

    Raises:
        e: An error occurred during download_features
    """
    try:
        cur = conn.cursor()
        table_full_name = f"{schema}.{table_name}"
        table_check_query = sql.SQL("SELECT to_regclass(%s)")
        cur.execute(table_check_query, [table_full_name])
        table_exists = cur.fetchone()[0]
        print(f"Table exists: {table_exists}")

        if table_exists:
            truncate_or_delete_table(table_name, service_name, schema, True)
        else:
            print(f"Table {schema}.{table_name} does not exist.")
            # Optionally, create the table dynamically here if necessary

        cur.close()

        start = 0
        total_imported = 0

        while True:
            esri_json = fetch_data(api_url + '/query', start, batch_size)
            if not esri_json or 'features' not in esri_json or not esri_json['features']:
                print("No more data or fetch failed.")
                break

            print(f"Processing batch from offset {start}, size {batch_size}. Features in batch: {len(esri_json['features'])}")

            geojson = esri_to_geojson(esri_json)
            geojson_str = json.dumps(geojson)

            tempdir = './tmp'
            os.makedirs(tempdir, exist_ok=True)
            with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.geojson', dir=tempdir) as tmp_file:
                tmp_file.write(geojson_str)
                tmp_file_path = tmp_file.name

            run_ogr2ogr(tmp_file_path, service_name, schema, table_name,
                        geometry_name, oid, source_epsg, target_epsg)

            os.remove(tmp_file_path)

            processed_features = len(geojson['features'])
            total_imported += processed_features

            print(f"Processed {processed_features} features in current batch. Total processed: {total_imported}")

            start += processed_features

            if processed_features < batch_size:
                print("Last batch processed, terminating loop.")
                break

        print(f"Total features imported: {total_imported}")

    except Exception as e:
        print(f"An error occurred during download_features: {e}")
        print(traceback.format_exc())
        raise e

def download_attachments(conn: connect, table_name: str, schema: str, service_name: str, api_url: str, oid: str, batch_size: int) -> None:
    """Download features attachments from the ArcGIS REST API and import them into PostgreSQL.

    Args:
        conn (connect): Database connection.
        table_name (str): Name of the parent table for features.
        schema (str):  Database schema for the parent table.
        service_name (str): PostgreSQL service name.
        api_url (str): URL of the ArcGIS REST API.
        geometry_name (str): Name of the geometry column.
        oid (str): Name of the object ID column.
        source_epsg (int): Source EPSG code for spatial reference.
        target_epsg (int): Target EPSG code for spatial reference transformation.
        batch_size (int): Number of records to fetch in each batch.

    Raises:
        e: An error occurred during download_features
    """
    try:
        cur = conn.cursor()
        parent_table = f"{schema}.{table_name}"
        attachment_table = f"{schema}.{table_name}_attach"
        temp_table = f"_{table_name}_attach"

        table_check_query = sql.SQL("SELECT to_regclass(%s)")
        cur.execute(table_check_query, [attachment_table])
        table_exists = cur.fetchone()[0]
        print(f"Table exists: {table_exists}")

        if table_exists:
            truncate_or_delete_table(attachment_table[len(schema)+1:], service_name, schema)
        else:
            print(f"Table {attachment_table} does not exist.")

            # Optionally, create the table dynamically here if necessary
            create_table = f"""
            CREATE TABLE {attachment_table} (
                OBJECTID BIGSERIAL PRIMARY KEY,
                PARENTID BIGINT references {parent_table}({oid}),
                ATTACHMENTID BIGINT,
                PARENT_OID BIGINT,
                PARENT_GLOBALID VARCHAR(255),
                NAME VARCHAR(255),
                SIZE BIGINT,
                CONTENT_TYPE VARCHAR(255),
                EXIF_INFO JSONB,
                KEYWORDS VARCHAR(255),
                URL VARCHAR(2083)
            );
            """
            cur.execute(create_table)

        start = 0
        total_imported = 0

        while True:
            esri_json = fetch_attachment_data(api_url, start, batch_size)
            if not esri_json or 'attachmentGroups' not in esri_json or not esri_json['attachmentGroups']:
                print("No more data or fetch failed.")
                break

            groups = esri_json['attachmentGroups']
            print(f"Processing batch from offset {start}, size {batch_size}. Features in batch: {len(groups)}")

            records = []
            for group in groups:
                print(group)
                parent_oid = group['parentObjectId']
                parent_globalid = group['parentGlobalId']
                for attachment in group['attachmentInfos']:
                    records.append((attachment['id'], parent_oid, parent_globalid, attachment['name'], attachment['size'], attachment['contentType'], json.dumps(attachment['exifInfo']), attachment['keywords'], attachment['url']))

            print(records)
            if len(records) > 0:
                create_table = f"""
                CREATE TEMP TABLE {temp_table} (
                    ATTACHMENTID BIGINT,
                    PARENT_OID BIGINT,
                    PARENT_GLOBALID VARCHAR(255),
                    NAME VARCHAR(255),
                    SIZE BIGINT,
                    CONTENT_TYPE VARCHAR(255),
                    EXIF_INFO JSONB,
                    KEYWORDS VARCHAR(255),
                    URL VARCHAR(2083)
                );
                """
                cur.execute(create_table)

                execute_values(cur,
                    f"INSERT INTO {temp_table} (attachmentid, parent_oid, parent_globalid, name, size, content_type, exif_info, keywords, url) VALUES %s",
                    records)

                update_table = f"""
                INSERT INTO {attachment_table}(
                    parentid,
                    attachmentid,
                    parent_oid,
                    parent_globalid,
                    name,
                    size,
                    content_type,
                    exif_info,
                    keywords,
                    url)
                SELECT 
                    (SELECT {oid} FROM {parent_table} WHERE {oid} = parent_oid) parentid, 
                    attachmentid,
                    parent_oid,
                    parent_globalid,
                    name,
                    size,
                    content_type,
                    exif_info,
                    keywords,
                    url
                From {temp_table}
                """
                cur.execute(update_table)

            processed_features = len(records)
            total_imported += processed_features

            print(f"Processed {processed_features} features in current batch. Total processed: {total_imported}")

            start += processed_features

            if processed_features < batch_size:
                print("Last batch processed, terminating loop.")
                break

        print(f"Total features imported: {total_imported}")

    except Exception as e:
        print(f"An error occurred during download_attachments: {e}")
        print(traceback.format_exc())
        raise e
    finally:
        cur.close()

def _download_file_bytes(url: str) -> bytes:
    try:
        response = requests.get(url, params={'token': _get_token()})
        return BytesIO(response.content)
    except requests.exceptions.RequestException as e:
        print("Error downloading the file:", e)

def _upload_file_bytes(content: bytes, bucket: Bucket, target_file_path: str):
    blob = bucket.blob(target_file_path)
    blob.upload_from_file(content)

    print(
        f"File uploaded to {target_file_path}."
    )

def transfer_attachments(conn: connect, table_name: str, schema: str, bucket_name: str):
    """_summary_

    Args:
        conn (connect): Database connect.  
        table_name (str): Name of the parent table for attachments
        schema (str): Schema for the parent table
        bucket_name (str): Name of the GCP bucket

    Raises:
        e: An error occurred during transfering attachments
        e: An error occurred getting bucket and database table
    """
    try:
        cur = conn.cursor()
        attachment_table = f"{schema}.{table_name}_attach"

        table_check_query = sql.SQL("SELECT to_regclass(%s)")
        cur.execute(table_check_query, [attachment_table])
        table_exists = cur.fetchone()[0]
        print(f"Table exists: {table_exists}")

        bucket = get_gcs_bucket(bucket_name)
        blobs_to_delete = [blob.name for blob in bucket.list_blobs(prefix=table_name)]
        bucket.delete_blobs(blobs_to_delete)

        if table_exists:
            cur = conn.cursor()
            update_cur = conn.cursor()

            # Execute a SELECT query
            cur.execute(f"SELECT objectid, attachmentid, name, url FROM {attachment_table}")

            record_count = 0
            try:
                row = cur.fetchone()
                while row is not None:
                    record_count += 1
                    objectid = row[0]
                    attachmentid = row[1]
                    file_name = row[2]
                    url = row[3]

                    content = _download_file_bytes(url)
                    target = f'{table_name}/{attachmentid}/{file_name}'

                    _upload_file_bytes(content, bucket, target)

                    # Execute a UPDATE query
                    update_cur.execute(f"UPDATE {attachment_table} SET url = 'https://storage.cloud.google.com/{bucket_name}/{target}' WHERE objectid = {objectid}")
                    conn.commit()

                    row = cur.fetchone()

                cur.close()
            except Exception as e:
                print(f"An error occurred during transfering attachments: {e}")
                print(traceback.format_exc())
                raise e
            finally:
                update_cur.close()

            print(f"Total attachments transferred: {record_count}")

    except Exception as e:
        print(f"An error occurred getting bucket and database table: {e}")
        print(traceback.format_exc())
        raise e
    finally:
        cur.close()

def main():
    parser = argparse.ArgumentParser(description='Process and import GeoJSON into PostgreSQL.')
    parser.add_argument('service_name', help='PostgreSQL service name')
    parser.add_argument('api_url', help='API URL to fetch data from')
    parser.add_argument('table_name', help='Table name to process data into')
    parser.add_argument('--schema', default='public', help='Database schema (default: public)')
    parser.add_argument('--geometry_name', default='geom', help='Geometry column name (default: geom)')
    parser.add_argument('--oid', default='OBJECTID', help='Feature ID field name (default: OBJECTID)')
    parser.add_argument('--source_epsg', type=int, help='Source EPSG code for spatial reference override')
    parser.add_argument('--target_epsg', type=int, help='Target EPSG code for spatial reference transformation')
    parser.add_argument('--batch', type=int, default=1000, help='Batch size for data fetching (default: 1000)')
    parser.add_argument('--save_attachments', type=bool, default=False, help='Save attachments to bucket (default: False)')
    parser.add_argument('--PGSERVICEFILE', type=str, default=None, help='PGSERVICEFILE (default: False)')
    parser.add_argument('--bucket_name', type=str, default=None, help='GCP Bucket name (Required if save-attachments is true)')

    args = parser.parse_args()
    print(f"Parsed arguments: {args}")

    setup_environment(args.PGSERVICEFILE)
    print("Environment setup complete")

    try:
        conn = connect(f"service={args.service_name}")
        conn.autocommit = True
        print(f"Connected to database using service: {args.service_name}")
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        return

    if args.source_epsg:
        source_epsg = args.source_epsg
    else:
        source_epsg = fetch_source_epsg(args.api_url)
        if source_epsg is None:
            print("Unable to determine source EPSG code. Exiting.")
            return

    try:
        download_features(
            conn,
            args.table_name,
            args.schema,
            args.service_name,
            args.api_url,
            args.geometry_name,
            args.oid,
            source_epsg,
            args.target_epsg,
            args.batch)

        if not args.save_attachments:
            print("Not saving attachments")
        else:
            print(f"Saving attachments to {args.bucket_name}")

            download_attachments(conn,
                args.table_name,
                args.schema,
                args.service_name,
                args.api_url,
                args.oid,
                args.batch)

            transfer_attachments(conn,
                args.table_name,
                args.schema,
                args.bucket_name)

    except Exception as e:
        print(f"An error occurred during processing: {e}")
        print(traceback.format_exc())

    finally:
        try:
            conn.close()
            print("Database connection closed")
        except Exception as e:
            print(f"Failed to close database connection: {e}")

if __name__ == "__main__":
    main()
