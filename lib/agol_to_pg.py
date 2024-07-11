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

import json
import requests
import argparse
import tempfile
import os
import subprocess
from psycopg2 import sql, connect
from esri_to_geojson import esri_to_geojson
from sql import truncate_or_delete_table
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

def setup_environment():
    os.environ['PGSERVICEFILE'] = '/app/env/pg_service.conf'

def fetch_data(url, start, count):
    params = {
        'f': 'json',
        'where': '1=1',  # A condition that's always true
        'outFields': '*',  # Fetch all fields
        'resultOffset': start,
        'resultRecordCount': count
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch data:", response.text)
        return None

def fetch_source_epsg(api_url):
    metadata_url = f"{api_url}?f=json"
    response = requests.get(metadata_url)

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

def run_ogr2ogr(geojson_file_path, service, schema, table_name, geometry_name, oid, source_epsg, target_epsg):
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

    args = parser.parse_args()

    print(f"Parsed arguments: {args}")

    setup_environment()
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

    print(f"Source EPSG: {source_epsg}")

    try:
        cur = conn.cursor()
        table_full_name = f"{args.schema}.{args.table_name}"
        table_check_query = sql.SQL("SELECT to_regclass(%s)")
        cur.execute(table_check_query, [table_full_name])
        table_exists = cur.fetchone()[0]
        print(f"Table exists: {table_exists}")

        if table_exists:
            truncate_or_delete_table(args.table_name, args.service_name, args.schema)
        else:
            print(f"Table {args.schema}.{args.table_name} does not exist.")
            # Optionally, create the table dynamically here if necessary

        cur.close()

        start = 0
        batch_size = int(args.batch)
        total_imported = 0

        while True:
            esri_json = fetch_data(args.api_url + '/query', start, batch_size)
            if not esri_json or 'features' not in esri_json or not esri_json['features']:
                print("No more data or fetch failed.")
                break

            print(f"Processing batch from offset {start}, size {batch_size}. Features in batch: {len(esri_json['features'])}")

            geojson = esri_to_geojson(esri_json)
            geojson_str = json.dumps(geojson)

            with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.geojson', dir='./tmp') as tmp_file:
                tmp_file.write(geojson_str)
                tmp_file_path = tmp_file.name

            run_ogr2ogr(tmp_file_path, args.service_name, args.schema, args.table_name,
                        args.geometry_name, args.oid, source_epsg, args.target_epsg)

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
