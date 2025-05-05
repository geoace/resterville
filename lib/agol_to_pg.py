"""
This file is part of RESTerville, a Workflow Automation toolkit.
Copyright (C) 2024  GEOACE

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.

You can contact the developer via email or using the contact form provided at https://geoace.net
"""
import json
import logging
import os
import re
import subprocess
import tempfile
import traceback
from types import SimpleNamespace
from typing import Any, Generator, Union

import requests
from arcgis.gis import GIS
from flask import Blueprint, Response, abort, request, stream_with_context
from google.cloud.storage import Bucket
from psycopg2 import connect, sql
from psycopg2.extras import execute_values

from lib.esri_to_geojson import esri_to_geojson
from lib.gcp import get_gcs_bucket
from lib.sql import truncate_or_delete_table

agol_to_pg = Blueprint('agol_to_pg', __name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def setup_environment(path: Union[str, None] = None) -> None:
    """Set up the environment for PostgreSQL connection."""
    if 'PGSERVICEFILE' not in os.environ:
        os.environ['PGSERVICEFILE'] = '/app/env/pg_service.conf' if path is None else path


def debug(msg: Any) -> str:
    """Log a debug message and return it."""
    logger.debug(msg)
    if logger.getEffectiveLevel() <= logging.DEBUG:
        return f"\n{msg}"
    return "."


def info(msg: Any) -> str:
    """Log an info message and return it."""
    logger.info(msg)
    if logger.getEffectiveLevel() <= logging.INFO:
        return f"\n{msg}"
    return "."


def error(msg: Any) -> str:
    """Log an error message and return it."""
    logger.error(msg)
    if logger.getEffectiveLevel() <= logging.ERROR:
        return f"\n{msg}"
    return "."


def _get_token() -> str:
    portal_url = os.getenv('ARCGIS_PORTAL_URL')
    user = os.getenv('ARCGIS_USER')
    password = os.getenv('ARCGIS_PASSWORD')
    gis = GIS(portal_url, user, password)

    return gis.session.auth.token


def _fetch_data(url: str, start: int, count: int) -> Union[dict, None]:
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
    response = requests.get(
        url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        abort(400, f"Failed to fetch_data from {url}. {response.text}")
        return None


def _fetch_attachment_data(url: str, start: int, count: int) -> Union[dict, None]:
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
    response = requests.get(f'{url}/queryAttachments',
                            params=params)  # pylint: disable=missing-timeout
    if response.status_code == 200:
        return response.json()
    else:
        logger.info("Failed to fetch: %s", response.text)
        return None


def _fetch_source_epsg(url: str) -> Union[int, None]:
    """Fetch the source EPSG code from the metadata of the ArcGIS REST API.

    Args:
        url (str): The URL of the ArcGIS REST API.

    Returns:
        Union[str, None]: The source EPSG code if found, otherwise None.
    """
    metadata_url = f"{url}?f=json"
    response = requests.get(metadata_url, params={
                            'token': _get_token()})  # pylint: disable=missing-timeout

    if response.status_code == 200:
        metadata = response.json()
        spatial_ref = metadata.get("extent", {}).get("spatialReference", {})

        if "latestWkid" in spatial_ref:
            return spatial_ref["latestWkid"]
        elif "wkid" in spatial_ref:
            return spatial_ref["wkid"]
        else:
            logger.info("Spatial reference not found in the metadata.")
            return None
    else:
        logger.info(
            "Failed to fetch source EPSG from %s: %s", metadata_url, response.text)
        return None


def _fetch_geometry_type(url: str) -> Union[str, None]:
    """Fetch the geometry type from the metadata of the ArcGIS REST API.

    Args:
        url (str): The URL of the ArcGIS REST API.
    Returns:
        Union[str, None]: The geometry type if found, otherwise None.
    """
    metadata_url = f"{url}?f=json"
    response = requests.get(metadata_url, params={
                            'token': _get_token()})  # pylint: disable=missing-timeout

    if response.status_code == 200:
        metadata = response.json()
        return metadata.get("geometryType", None)

    else:
        logger.info(
            "Failed to fetch geometry type from %s: %s", metadata_url, response.text)
        return None


def _fetch_metadata(url: str, fields: Union[list[str], None] = None) -> Union[dict[str, Any], None]:
    """Fetch the specified fields from the metadata of the ArcGIS REST API.

    Args:
        url (str): The URL of the ArcGIS REST API.
        fields (Union[list[str], None], optional): A list of metadata fields to fetch. If None, the entire metadata is returned.

    Returns:
        Union[dict[str, Any], None]: A dictionary containing the requested metadata fields (or the entire metadata if no fields are specified), or None if the request fails.
    """
    metadata_url = f"{url}?f=json"
    response = requests.get(metadata_url, params={
                            'token': _get_token()})  # pylint: disable=missing-timeout

    if response.status_code == 200:
        metadata = response.json()

        retval = {}
        if fields and len(fields) > 0:
            for field in fields:
                retval[field] = metadata.get(field, None)
        else:
            retval = metadata

        return retval

    else:
        logger.info(
            "Failed to fetch metadata from %s: %s", metadata_url, response.text)
        return None


def _check_oid(url: str, oid: str) -> Union[int, None]:
    """Fetch the source EPSG code from the metadata of the ArcGIS REST API.

    Args:
        url (str): The URL of the ArcGIS REST API.

    Returns:
        Union[str, None]: The source EPSG code if found, otherwise None.
    """
    metadata_url = f"{url}?f=json"
    response = requests.get(metadata_url, params={
                            'token': _get_token()})  # pylint: disable=missing-timeout

    if response.status_code == 200:
        metadata = response.json()
        fields = metadata.get("fields", {})

        for field in fields:
            if field['name'].lower() == oid.lower():
                abort(400,
                      description=f"Field {oid} already exists in the service.  Define a unique field to be used for the OID.")


def _run_ogr2ogr(geojson_file_path: str,
                 service: str,
                 schema: str,
                 table: str,
                 oid: str,
                 geometry_name: Union[str, None] = None,
                 source_epsg: Union[int, None] = None,
                 target_epsg: Union[int, None] = None):
    """Run the ogr2ogr command to import GeoJSON data into PostgreSQL.

    Args:
        geojson_file_path (str)
        service (str)
        schema (str)
        table (str)
        geometry_name (str)
        oid (str)
        source_epsg (int)
        target_epsg (int)
    """
    with open(geojson_file_path, 'r') as geojson_file:  # pylint: disable=unspecified-encoding
        geojson_data = json.load(geojson_file)
        if "features" not in geojson_data:
            logger.info("No features found in the provided GeoJSON file.")
            return

        command = [
            'ogr2ogr',
            '-progress',
            '--config', 'PG_USE_COPY', 'YES',
            '-f', 'PostgreSQL',
            f"PG:service={service} sslmode=disable active_schema={schema}",
            geojson_file_path,
            '-append',
            '-lco', 'FID=' + oid,
            '-nln', schema + '.' + table
        ]

        geom_nlt = "NONE"

        if geometry_name and len(geometry_name) > 0:
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

            command += ['-lco', f'GEOMETRY_NAME={geometry_name}']
            command += ['-lco', 'GEOMETRY_TYPE=geometry']
            command += ['-lco', 'DIM=2']

            if source_epsg:
                command += ['-a_srs', f'EPSG:{source_epsg}']
            if target_epsg:
                command += ['-t_srs', f'EPSG:{target_epsg}']

        command += ['-nlt', geom_nlt]
    logger.info(
        "Running ogr2ogr command: %s", ' '.join(command))

    process = subprocess.run(
        command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)

    if process.returncode != 0:
        logger.info("ogr2ogr command failed: %s", process.stderr)
    else:
        logger.info("ogr2ogr command was successful")


def _stream_to_gcs(url: str, bucket: Bucket, target_file_path: str):
    """Stream data from a URL to a Google Cloud Storage bucket."""
    try:
        blob = bucket.blob(target_file_path)

        response = requests.get(
            url, params={'token': _get_token()}, stream=True)
        response.raise_for_status()

        with blob.open("wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logging.info(
            "Successfully streamed data from %s to gs://%s/%s", url, bucket.name, target_file_path)

    except requests.exceptions.RequestException as e:
        logging.info(
            "Error occurred while fetching data from %s in stream_to_gcs: %s", url, e)
        raise e
    except Exception as e:
        logging.info("An error occurred in stream_to_gcs: %s", e)
        raise e


def download_features(
        conn,
        table: str,
        schema: str,
        service_name: str,
        url: str,
        oid: str,
        batch_size: int,
        has_geometry: bool,
        geometry_name: Union[str, None] = None,
        source_epsg: Union[int, None] = None,
        target_epsg: Union[int, None] = None) -> Generator[str, None, None]:
    """Download feature records from the ArcGIS REST API and import them into PostgreSQL.

    Args:
        conn (connect): Database connection.
        table (str): Name of the table to import data into.
        schema (str):  Database schema for the table.
        service_name (str): PostgreSQL service name.
        url (str): URL of the ArcGIS REST API.
        has_geometry (bool): Indicates if the data has geometry.
        geometry_name (str): Name of the geometry column.
        oid (str): Name of the object ID column.
        source_epsg (int): Source EPSG code for spatial reference.
        target_epsg (int): Target EPSG code for spatial reference transformation.
        batch_size (int): Number of records to fetch in each batch.

    Raises:
        e: An error occurred during download_features
    """
    try:
        yield info("Starting download feature records")
        with conn.cursor() as cur:
            table_full_name = f"{schema}.{table}"
            table_check_query = sql.SQL("SELECT to_regclass(%s)")
            cur.execute(table_check_query, [table_full_name])
            table_exists = cur.fetchone()[0]
            yield from debug(f"Table exists: {table_exists}")

            if table_exists:
                truncate_or_delete_table(
                    table, service_name, schema, True, reset_sequence=True)
            else:
                yield from debug(f"Table {schema}.{table} does not exist.")
                # Optionally, create the table dynamically here if necessary

        start = 0
        total_imported = 0

        while True:
            esri_json = _fetch_data(url + '/query', start, batch_size)
            if not esri_json or 'features' not in esri_json or not esri_json['features']:
                yield from debug("No more data or fetch failed.")
                break

            yield from info(f"Processing batch from offset {start}, size {batch_size}.")
            yield from info(f"Features in batch: {len(esri_json['features'])}")

            geojson = esri_to_geojson(
                esri_json, has_geometry=has_geometry)
            geojson_str = json.dumps(geojson)

            tempdir = './tmp'
            os.makedirs(tempdir, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                    mode='w+',
                    delete=False,
                    suffix='.geojson',
                    dir=tempdir) as tmp_file:

                tmp_file.write(geojson_str)
                tmp_file_path = tmp_file.name

            if has_geometry:
                _run_ogr2ogr(tmp_file_path, service_name, schema, table,
                             oid, geometry_name, source_epsg, target_epsg)
            else:
                _run_ogr2ogr(tmp_file_path, service_name, schema, table, oid)

            os.remove(tmp_file_path)

            processed_features = len(geojson['features'])
            total_imported += processed_features

            yield from debug(f"Processed {processed_features} features in current batch.")
            yield from debug(f"Total processed: {total_imported}")

            start += processed_features

            if processed_features < batch_size:
                logger.debug("Last batch processed, terminating loop.")
                break

        yield from info("Finished download feature records")
        yield from info(f"Total features imported: {total_imported}")

    except Exception as e:
        yield from error(f"An error occurred during download_features: {e}")
        raise e


def download_attachments(
        conn,
        table: str,
        schema: str,
        service_name: str,
        url: str, oid: str,
        batch_size: int) -> Generator[str, None, None]:
    """Download attachment records from the ArcGIS REST API and import them into PostgreSQL.

    Args:
        conn (connect): Database connection.
        table (str): Name of the parent table for features.
        schema (str):  Database schema for the parent table.
        service_name (str): PostgreSQL service name.
        url (str): URL of the ArcGIS REST API.
        geometry_name (str): Name of the geometry column.
        batch_size (int): Number of records to fetch in each batch.

    Raises:
        e: An error occurred during download_features
    """
    try:
        yield from info("Starting download attachment records")

        with conn.cursor() as cur:
            parent_table = f"{schema}.{table}"
            attachment_table = f"{schema}.{table}_attach"
            temp_table = f"_{table}_attach"

            table_check_query = sql.SQL("SELECT to_regclass(%s)")
            cur.execute(table_check_query, [attachment_table])
            table_exists = cur.fetchone()[0]
            yield from debug(f"Table exists: {table_exists}")

            if table_exists:
                truncate_or_delete_table(
                    attachment_table[len(schema)+1:], service_name, schema, reset_sequence=True)
            else:
                yield from debug(f"Table {attachment_table} does not exist.")

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
                esri_json = _fetch_attachment_data(url, start, batch_size)
                if not esri_json \
                        or 'attachmentGroups' not in esri_json \
                        or not esri_json['attachmentGroups']:
                    yield from debug("No more data or fetch failed.")
                    break

                groups = esri_json['attachmentGroups']
                yield from info(f"Processing batch from offset {start}, size {batch_size}.")
                yield from info(f"Features in batch: {len(groups)}")

                records = []
                for group in groups:
                    parent_oid = group['parentObjectId']
                    parent_globalid = group['parentGlobalId']
                    for attachment in group['attachmentInfos']:
                        records.append((attachment['id'],
                                        parent_oid,
                                        parent_globalid,
                                        attachment['name'],
                                        attachment['size'],
                                        attachment['contentType'],
                                        json.dumps(attachment['exifInfo']),
                                        attachment['keywords'],
                                        attachment['url']))

                if len(records) > 0:
                    create_table = f"""
                    CREATE TEMP TABLE IF NOT EXISTS {temp_table} (
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
                                   f"""INSERT INTO {temp_table} (
                                        attachmentid, 
                                        parent_oid, 
                                        parent_globalid, 
                                        name, 
                                        size, 
                                        content_type, 
                                        exif_info, 
                                        keywords, 
                                        url) VALUES %s""",
                                   records)

                    identifiers = _fetch_metadata(
                        url, ['globalIdField', 'objectIdField'])
                    if not identifiers or len(identifiers) < 1:
                        yield from error("Unable to get globalIdField or objectIdField from feature service")
                    else:
                        parent_field = identifiers.get(
                            'globalIdField') if 'globalIdField' in identifiers else identifiers.get('objectIdField')
                        parent_lookup = 'parent_globalid' if 'globalIdField' in identifiers else 'parent_oid'

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
                            (SELECT {oid} 
                                FROM {parent_table} 
                                WHERE {parent_field} = {parent_lookup}) parentid,
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

                yield from debug(f"Processed {processed_features} features in current batch.")
                yield from debug(f"Total processed: {total_imported}")

                start += processed_features

                if processed_features < batch_size:
                    yield from debug("Last batch processed, terminating loop.")
                    break

        yield from info("Finished download attachment records")
        yield from info(f"Total features imported: {total_imported}")

    except Exception as e:
        yield from error(f"An error occurred during download_attachments: {e}")
        raise e


def transfer_attachments(
        conn,
        table: str,
        schema: str,
        bucket_name: str) -> Generator[str, None, None]:
    """Use attachment records from PostgreSQL to download attachments and stream them to a GCP bucket.

    Args:
        conn (connect): Database connect.
        table (str): Name of the parent table for attachments
        schema (str): Schema for the parent table
        bucket_name (str): Name of the GCP bucket

    """
    try:
        yield from info("Starting transfer attachments")
        record_count = 0

        with conn.cursor() as cur, conn.cursor() as update_cur:
            attachment_table = f"{schema}.{table}_attach"

            table_check_query = sql.SQL("SELECT to_regclass(%s)")
            cur.execute(table_check_query, [attachment_table])
            table_exists = cur.fetchone()[0]
            yield from debug(f"Attachment Table exists: {table_exists}")

            bucket = get_gcs_bucket(bucket_name)
            yield from debug(
                f"Connected to bucket: {bucket_name}")

            blobs = {blob.name: blob for blob in bucket.list_blobs(
                prefix=table)}

            yield from debug(
                f"Found {len(blobs)} {table} attachments in {bucket_name}")

            if table_exists:
                # Execute a SELECT query
                cur.execute(
                    f"SELECT objectid, attachmentid, name, url FROM {attachment_table}")

                try:
                    row = cur.fetchone()
                    while row is not None:
                        record_count += 1
                        objectid = row[0]
                        attachmentid = row[1]
                        file_name, extension = os.path.splitext(row[2])
                        file_name = re.sub(
                            r'[^a-zA-Z0-9]', '_', file_name) + extension
                        url = row[3]

                        target = f'{table}/{attachmentid}{extension}'

                        blob = blobs.pop(target, None)
                        if not blob:
                            _stream_to_gcs(url, bucket, target)
                            yield from debug(
                                f"Transferred {file_name} to {target}")
                        else:
                            yield from debug(
                                f"Attachments {target} already exists in bucket {bucket_name}")

                        # Execute a UPDATE query
                        update_cur.execute(
                            f"""
                        UPDATE {attachment_table}
                        SET url = 'https://storage.cloud.google.com/{bucket_name}/{target}',
                            name = '{file_name}'
                        WHERE objectid = {objectid}""")
                        conn.commit()

                        row = cur.fetchone()
                except Exception as e:
                    yield from error(
                        f"An error occurred during transfering attachments: {e}")
                    yield from error(traceback.format_exc())
                    raise e

                for blob in blobs.values():
                    blob.delete()
                    yield from debug(f"Deleted attachments: {blob.name}")

        yield from info(
            f"Finished transferring attachments to {bucket_name}")
        yield from info(
            f"Total attachments transferred: {record_count}")
        yield from info(f"Deleted {len(blobs)} attachments from bucket {bucket_name}")

    except Exception as e:
        yield from error(
            f"An error occurred getting bucket and database table: {e}")
        raise e


def parse_args() -> SimpleNamespace:
    """Parse command line arguments or request parameters."""

    args = request.args if request.method == 'GET' else request.form

    retval = SimpleNamespace()

    retval.loglevel = args.get('loglevel', 'info')
    retval.service_name = args.get('service')
    retval.url = args.get('url')
    retval.table = args.get('table')
    retval.oid = args.get('oid', 'agol_to_pg_oid')

    retval.schema = args.get('schema', 'public')
    retval.batch = int(args.get('batch', '1000'))
    retval.save_attachments = bool(json.dumps(
        args.get('save_attachments', "false")))
    retval.bucket_name = args.get('bucket', os.getenv('BUCKET'))

    if not retval.service_name or not retval.url or not retval.table or not retval.oid:
        abort(400, 'Missing required parameters (service, url, table, oid)')

    _check_oid(retval.url, retval.oid)

    if retval.save_attachments == "true" and not retval.bucket_name:
        abort(400, 'Missing required parameter (bucket) for saving attachments')

    retval.geometry_name = args.get('geometry_name', None)
    retval.source_epsg = args.get('source_epsg', None)
    retval.target_epsg = args.get('target_epsg', None)
    retval.has_geometry = bool(_fetch_geometry_type(retval.url))

    if retval.source_epsg:
        retval.source_epsg = int(retval.source_epsg)
    elif retval.has_geometry:
        retval.source_epsg = _fetch_source_epsg(retval.url)

    if retval.target_epsg:
        retval.target_epsg = int(retval.target_epsg)

    return retval


@agol_to_pg.route('/agol2pg', methods=['GET', 'POST'])
def run_pg_script():
    """ Run the AGOL to PostgreSQL script. """
    try:
        args = parse_args()
        if args.loglevel == 'debug':
            logger.setLevel(logging.DEBUG)

        def generator():
            try:
                logging.info("Starting AGOL to PostgreSQL script")
                yield "Starting AGOL to PostgreSQL script"
                yield from debug(args)

                setup_environment()
                yield from debug("Environment setup complete")

                with connect(f"service={args.service_name}") as conn:
                    conn.autocommit = True
                    yield from debug(f"Connected to database using service: {args.service_name}")

                    if args.has_geometry and not args.source_epsg:
                        yield from info("WARNING: No source_epsg was provided and it could not be discovered from the services metadata.")

                    for line in download_features(
                            conn=conn,
                            table=args.table,
                            schema=args.schema,
                            service_name=args.service_name,
                            url=args.url,
                            oid=args.oid,
                            batch_size=args.batch,
                            has_geometry=args.has_geometry,
                            geometry_name=args.geometry_name,
                            source_epsg=args.source_epsg,
                            target_epsg=args.target_epsg):
                        yield line

                    for line in download_attachments(
                            conn,
                            args.table,
                            args.schema,
                            args.service_name,
                            args.url,
                            args.oid,
                            args.batch):
                        yield line

                    try:
                        for line in transfer_attachments(conn,
                                                         args.table,
                                                         args.schema,
                                                         args.bucket_name):
                            yield line
                    except Exception as e:
                        yield from error(e)

                yield from info("Finished AGOL to PostgreSQL script")

            except Exception as e:
                yield from error(f"An error occurred: {e}")
                yield from error(traceback.format_exc())

        return Response(stream_with_context(generator()), mimetype='text/event-stream')
    except Exception as e:
        logger.error("An error occurred: %s", e)
        logger.error(traceback.format_exc())
        abort(500, f"An internal error occurred: {str(e)}")
