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
import contextlib
import json
import logging
import os
import re
import subprocess
import tempfile
import time
import traceback
from types import SimpleNamespace
from typing import Any, Generator, Optional, Union

import requests
from flask import Blueprint, Response, request, stream_with_context
from google.cloud.storage import Bucket
from psycopg2 import connect, sql
from psycopg2.extras import execute_values

from lib import security
from lib.esri_to_geojson import esri_to_geojson
from lib.gcp import get_gcs_bucket
from lib.sql import truncate_or_delete_table

agol_to_pg = Blueprint('agol_to_pg', __name__)

logger = logging.getLogger(__name__)

# ArcGIS geometryType -> ogr2ogr -nlt. Lines and polygons are always promoted to
# multi so a layer that mixes single and multi parts loads into one table.
ESRI_TO_OGR_GEOMETRY = {
    'esriGeometryPoint': 'POINT',
    'esriGeometryMultipoint': 'MULTIPOINT',
    'esriGeometryPolyline': 'MULTILINESTRING',
    'esriGeometryPolygon': 'MULTIPOLYGON',
}


class ArcGISRequestError(Exception):
    """Raised when an ArcGIS REST request fails or returns an error payload."""


def setup_environment(path: Union[str, None] = None) -> None:
    """Set up the environment for PostgreSQL connection."""
    if 'PGSERVICEFILE' not in os.environ:
        os.environ['PGSERVICEFILE'] = '/app/env/pg_service.conf' if path is None else path


def _emit(level: int, msg: Any) -> str:
    logger.log(level, msg)
    return f"\n{msg}" if logger.isEnabledFor(level) else "."


def debug(msg: Any) -> str:
    """Log a debug message and return it for the response stream."""
    return _emit(logging.DEBUG, msg)


def info(msg: Any) -> str:
    """Log an info message and return it for the response stream."""
    return _emit(logging.INFO, msg)


def error(msg: Any) -> str:
    """Log an error message and return it for the response stream."""
    return _emit(logging.ERROR, msg)


# ---------------------------------------------------------------------------
# ArcGIS REST
# ---------------------------------------------------------------------------

_TOKEN_CACHE = {"token": None, "expires": 0.0}


def _get_token_cached() -> Optional[str]:
    """Fetch and cache an ArcGIS token, only when a secured service needs one."""
    portal_url = os.getenv('ARCGIS_PORTAL_URL')
    user = os.getenv('ARCGIS_USER')
    password = os.getenv('ARCGIS_PASSWORD')

    if not all([portal_url, user, password]):
        return None

    if _TOKEN_CACHE["token"] and time.time() < _TOKEN_CACHE["expires"]:
        return _TOKEN_CACHE["token"]

    try:
        from arcgis.gis import GIS  # heavy import, only needed for secured services
        logger.info("Fetching ArcGIS token (secured service detected)")
        gis = GIS(portal_url, user, password)
        token = gis.session.auth.token
        _TOKEN_CACHE["token"] = token
        _TOKEN_CACHE["expires"] = time.time() + (25 * 60)
        return token
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.warning("Could not authenticate to Portal: %s", e)
        return None


def _parse_response(response: requests.Response) -> dict:
    if response.status_code != 200:
        raise ArcGISRequestError(
            f"ArcGIS request failed with HTTP {response.status_code}: {response.text[:500]}")
    try:
        return response.json()
    except ValueError as e:
        raise ArcGISRequestError(f"ArcGIS returned a non-JSON response: {response.text[:500]}") from e


def arcgis_request(url: str, params: dict) -> dict:
    """Make an ArcGIS REST request, retrying with a token if the service is secured.

    The token is only sent to hosts allowed by security.token_allowed(), and it
    goes in a POST body so it does not end up in URLs or request logs.
    """
    params = {'f': 'json', **params}
    data = _parse_response(requests.get(url, params=params, timeout=security.HTTP_TIMEOUT))

    if isinstance(data, dict) and "error" in data:
        code = data["error"].get("code")
        if code in (498, 499):
            if not security.token_allowed(url):
                raise ArcGISRequestError(
                    "Service requires a token, but its host is not ArcGIS Online, "
                    "ARCGIS_PORTAL_URL, or listed in ARCGIS_TOKEN_HOSTS.")
            token = _get_token_cached()
            if not token:
                raise ArcGISRequestError("Service requires a token but authentication failed.")
            logger.info("Secured service detected; retrying with token")
            data = _parse_response(requests.post(
                url, data={**params, 'token': token}, timeout=security.HTTP_TIMEOUT))

    if isinstance(data, dict) and "error" in data:
        raise ArcGISRequestError(f"ArcGIS returned an error: {json.dumps(data['error'])[:500]}")
    return data


def _fetch_metadata(url: str) -> dict:
    """Fetch the layer's metadata (fields, geometry type, spatial reference, ...)."""
    return arcgis_request(url, {})


def _source_epsg(metadata: dict) -> Optional[int]:
    spatial_ref = (metadata.get("extent") or {}).get("spatialReference") or {}
    return spatial_ref.get("latestWkid") or spatial_ref.get("wkid")


def _check_oid(metadata: dict, oid: str) -> None:
    """The oid column is created by ogr2ogr, so it must not collide with a service field."""
    for field in metadata.get("fields") or []:
        if field['name'].lower() == oid.lower():
            raise security.ValidationError(
                f"Field {oid} already exists in the service. "
                "Define a unique field to be used for the OID.")


def _fetch_data(url: str, start: int, count: int, order_by: Optional[str]) -> dict:
    params = {
        'where': '1=1',
        'outFields': '*',
        'resultOffset': start,
        'resultRecordCount': count,
        'returnGeometry': 'true',
    }
    # Without a stable sort order, offset paging can skip or repeat records.
    if order_by:
        params['orderByFields'] = order_by
    return arcgis_request(f"{url}/query", params)


def _fetch_attachment_data(url: str, start: int, count: int) -> Optional[dict]:
    params = {
        'definitionExpression': '1=1',
        'returnUrl': 'true',
        'resultOffset': start,
        'resultRecordCount': count,
    }
    data = arcgis_request(f"{url}/queryAttachments", params)
    if not data or 'attachmentGroups' not in data:
        return None
    return data


# ---------------------------------------------------------------------------
# PostgreSQL loading
# ---------------------------------------------------------------------------

def _run_ogr2ogr(geojson_file_path: str,
                 service: str,
                 schema: str,
                 table: str,
                 oid: str,
                 geom_nlt: str = 'NONE',
                 geometry_name: Optional[str] = None,
                 source_epsg: Optional[int] = None,
                 target_epsg: Optional[int] = None,
                 append_mode: bool = True) -> None:
    """Run ogr2ogr to load a GeoJSON file into PostgreSQL.

    SSL behaviour comes from pg_service.conf or PGSSLMODE (libpq default:
    prefer). It used to be forced to sslmode=disable, which sent the database
    password in clear text to any remote server.
    """
    command = [
        'ogr2ogr',
        '--config', 'PG_USE_COPY', 'YES',
        '-f', 'PostgreSQL',
        f"PG:service={service} active_schema={schema}",
        geojson_file_path,
        '-nln', f'{schema}.{table}',
    ]
    spatial = bool(geometry_name) and geom_nlt != 'NONE'
    if append_mode:
        # Layer creation options only apply when ogr2ogr creates the table.
        command.append('-append')
    else:
        command += ['-lco', f'FID={oid}']
        if spatial:
            command += ['-lco', f'GEOMETRY_NAME={geometry_name}',
                        '-lco', 'GEOMETRY_TYPE=geometry',
                        '-lco', 'DIM=2']

    if spatial:
        if source_epsg:
            command += ['-a_srs', f'EPSG:{source_epsg}']
        if target_epsg:
            command += ['-t_srs', f'EPSG:{target_epsg}']
    else:
        geom_nlt = 'NONE'
    command += ['-nlt', geom_nlt]

    logger.info("Running ogr2ogr command: %s", ' '.join(command))
    process = subprocess.run(command, text=True, capture_output=True, check=False)
    if process.returncode != 0:
        detail = (process.stderr or process.stdout or '').strip()[-2000:]
        raise RuntimeError(f"ogr2ogr exited with status {process.returncode}: {detail}")
    if process.stderr.strip():
        logger.warning("ogr2ogr warnings: %s", process.stderr.strip()[-2000:])
    logger.info("ogr2ogr command was successful")


def _table_exists(cur, schema: str, table: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", [f'{schema}.{table}'])
    return cur.fetchone()[0] is not None


def _column_exists(cur, schema: str, table: str, column: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
        [schema, table, column])
    return cur.fetchone() is not None


def download_features(
        conn,
        table: str,
        schema: str,
        service_name: str,
        url: str,
        oid: str,
        batch_size: int,
        metadata: dict,
        geometry_name: Optional[str] = None,
        source_epsg: Optional[int] = None,
        target_epsg: Optional[int] = None) -> Generator[str, None, None]:
    """Download feature records from the ArcGIS REST API and import them into PostgreSQL."""
    yield info("Starting download feature records")

    # Geometry handling comes from the layer definition, not from whichever
    # feature happens to be first in a batch (it may have null geometry).
    geom_nlt = ESRI_TO_OGR_GEOMETRY.get(metadata.get('geometryType') or '', 'NONE')
    has_geometry = geom_nlt != 'NONE'
    geometry_name = (geometry_name or 'geom') if has_geometry else None
    order_by = metadata.get('objectIdField')

    with conn.cursor() as cur:
        table_exists = _table_exists(cur, schema, table)
        if table_exists:
            if not _column_exists(cur, schema, table, oid):
                raise RuntimeError(
                    f"{schema}.{table} exists but has no '{oid}' column, so it was not created "
                    f"with oid={oid}. Drop the table (and {table}_attach) and re-run, or pass "
                    "the table's existing primary key as oid.")
            yield debug(f"Table exists. Truncating {schema}.{table}")
            # CASCADE also clears the {table}_attach table that references it.
            truncate_or_delete_table(table, service_name, schema, cascade=True, reset_sequence=True)
        else:
            yield debug(f"Table {schema}.{table} missing. Will be created.")

    start = 0
    total_imported = 0

    while True:
        esri_json = _fetch_data(url, start, batch_size, order_by)
        features = esri_json.get('features')
        if features is None:
            raise ArcGISRequestError(f"Server response missing 'features' key: {str(esri_json)[:500]}")
        if not features:
            yield debug("No features found in this batch (batch is empty).")
            break

        yield info(f"Processing batch from offset {start}, {len(features)} features.")
        logger.debug("Sample feature: %s", json.dumps(features[0])[:2000])

        geojson = esri_to_geojson(esri_json, has_geometry=has_geometry)

        fd, tmp_file_path = tempfile.mkstemp(suffix='.geojson')
        try:
            with os.fdopen(fd, 'w') as tmp_file:
                json.dump(geojson, tmp_file)

            # If the table existed at the start it was truncated, so always append.
            # Otherwise the first batch creates it and later batches append.
            _run_ogr2ogr(
                tmp_file_path, service_name, schema, table, oid,
                geom_nlt=geom_nlt,
                geometry_name=geometry_name,
                source_epsg=source_epsg,
                target_epsg=target_epsg,
                append_mode=table_exists or total_imported > 0)
        finally:
            os.remove(tmp_file_path)

        total_imported += len(features)
        start += len(features)
        yield debug(f"Total processed: {total_imported}")

        # A page smaller than batch_size is not proof of the end: the service may
        # cap pages at its maxRecordCount. exceededTransferLimit says more remain.
        if not esri_json.get('exceededTransferLimit') and len(features) < batch_size:
            break

    yield info("Finished download feature records")
    yield info(f"Total features imported: {total_imported}")


def download_attachments(
        conn,
        table: str,
        schema: str,
        service_name: str,
        url: str,
        oid: str,
        batch_size: int,
        metadata: dict) -> Generator[str, None, None]:
    """Download attachment records from the ArcGIS REST API and import them into PostgreSQL."""
    yield info("Starting download attachment records")

    attach_table = f"{table}_attach"
    parent_ident = sql.Identifier(schema, table)
    attach_ident = sql.Identifier(schema, attach_table)
    temp_ident = sql.Identifier(f"_{table}_attach")

    # ogr2ogr launders field names to lowercase.
    global_id_field = (metadata.get('globalIdField') or '').lower()
    object_id_field = (metadata.get('objectIdField') or '').lower()
    if global_id_field:
        parent_field, parent_lookup = global_id_field, 'parent_globalid'
    elif object_id_field:
        parent_field, parent_lookup = object_id_field, 'parent_oid'
    else:
        raise RuntimeError("Layer has neither globalIdField nor objectIdField; cannot link attachments.")

    with conn.cursor() as cur:
        if not _column_exists(cur, schema, table, oid):
            raise RuntimeError(
                f"{schema}.{table} has no '{oid}' column to reference from {attach_table}. "
                f"Drop {schema}.{table} and re-run so it is created with oid={oid}.")
        if not _column_exists(cur, schema, table, parent_field):
            raise RuntimeError(f"{schema}.{table} has no '{parent_field}' column to match attachments on.")

        if _table_exists(cur, schema, attach_table):
            truncate_or_delete_table(attach_table, service_name, schema, reset_sequence=True)
        else:
            yield debug(f"Creating {schema}.{attach_table}")
            cur.execute(sql.SQL("""
                CREATE TABLE {attach} (
                    objectid BIGSERIAL PRIMARY KEY,
                    parentid BIGINT REFERENCES {parent}({oid}),
                    attachmentid BIGINT,
                    parent_oid BIGINT,
                    parent_globalid VARCHAR(255),
                    name VARCHAR(255),
                    size BIGINT,
                    content_type VARCHAR(255),
                    exif_info JSONB,
                    keywords VARCHAR(255),
                    url VARCHAR(2083)
                )""").format(attach=attach_ident, parent=parent_ident, oid=sql.Identifier(oid)))

        cur.execute(sql.SQL("""
            CREATE TEMP TABLE IF NOT EXISTS {temp} (
                attachmentid BIGINT,
                parent_oid BIGINT,
                parent_globalid VARCHAR(255),
                name VARCHAR(255),
                size BIGINT,
                content_type VARCHAR(255),
                exif_info JSONB,
                keywords VARCHAR(255),
                url VARCHAR(2083)
            )""").format(temp=temp_ident))

        insert_from_temp = sql.SQL("""
            INSERT INTO {attach} (parentid, attachmentid, parent_oid, parent_globalid,
                                  name, size, content_type, exif_info, keywords, url)
            SELECT (SELECT p.{oid} FROM {parent} p WHERE p.{pfield} = t.{plookup} LIMIT 1),
                   t.attachmentid, t.parent_oid, t.parent_globalid, t.name, t.size,
                   t.content_type, t.exif_info, t.keywords, t.url
            FROM {temp} t""").format(
            attach=attach_ident, parent=parent_ident, temp=temp_ident,
            oid=sql.Identifier(oid), pfield=sql.Identifier(parent_field),
            plookup=sql.Identifier(parent_lookup))

        start = 0
        total_imported = 0
        while True:
            try:
                esri_json = _fetch_attachment_data(url, start, batch_size)
            except ArcGISRequestError as e:
                yield error(f"Attachment query failed (are attachments enabled on this layer?): {e}")
                break
            if not esri_json or not esri_json['attachmentGroups']:
                yield debug("No more attachment data.")
                break

            groups = esri_json['attachmentGroups']
            records = []
            for group in groups:
                for attachment in group.get('attachmentInfos', []):
                    records.append((attachment.get('id'),
                                    group.get('parentObjectId'),
                                    group.get('parentGlobalId'),
                                    attachment.get('name'),
                                    attachment.get('size'),
                                    attachment.get('contentType'),
                                    json.dumps(attachment.get('exifInfo')),
                                    attachment.get('keywords'),
                                    attachment.get('url')))
            yield info(f"Processing attachment batch from offset {start}: {len(records)} attachments.")

            if records:
                # The temp table lives for the whole connection; clear it so the
                # previous batch is not inserted again.
                cur.execute(sql.SQL("TRUNCATE {temp}").format(temp=temp_ident))
                execute_values(cur, sql.SQL("""
                    INSERT INTO {temp} (attachmentid, parent_oid, parent_globalid, name, size,
                                        content_type, exif_info, keywords, url)
                    VALUES %s""").format(temp=temp_ident), records)
                cur.execute(insert_from_temp)

            total_imported += len(records)
            start += len(records)

            if not esri_json.get('exceededTransferLimit') and len(records) < batch_size:
                break

    yield info("Finished download attachment records")
    yield info(f"Total attachments imported: {total_imported}")


def _stream_to_gcs(url: str, bucket: Bucket, target_file_path: str) -> None:
    """Stream a file from a URL into a Google Cloud Storage bucket."""
    params = {}
    if security.token_allowed(url):
        token = _get_token_cached()
        if token:
            params['token'] = token

    with requests.get(url, params=params, stream=True, timeout=security.HTTP_TIMEOUT) as response:
        response.raise_for_status()
        blob = bucket.blob(target_file_path)
        with blob.open("wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
    logger.info("Streamed attachment to gs://%s/%s", bucket.name, target_file_path)


def transfer_attachments(conn, table: str, schema: str, bucket_name: str) -> Generator[str, None, None]:
    """Copy attachments listed in {table}_attach into a GCS bucket and repoint their URLs."""
    yield info("Starting transfer attachments")
    attach_table = f"{table}_attach"
    attach_ident = sql.Identifier(schema, attach_table)
    record_count = 0

    with conn.cursor() as cur:
        if not _table_exists(cur, schema, attach_table):
            yield info(f"No {schema}.{attach_table} table; nothing to transfer.")
            return

        bucket = get_gcs_bucket(bucket_name)
        # Trailing slash: without it, the prefix for "hydrant" also matches
        # "hydrant_inspections/...", and those blobs were deleted as orphans.
        prefix = f"{table}/"
        blobs = {blob.name: blob for blob in bucket.list_blobs(prefix=prefix)}
        yield debug(f"Found {len(blobs)} {table} attachments in {bucket_name}")

        cur.execute(sql.SQL("SELECT objectid, attachmentid, name, url FROM {}").format(attach_ident))
        rows = cur.fetchall()

        update = sql.SQL("UPDATE {} SET url = %s, name = %s WHERE objectid = %s").format(attach_ident)
        for objectid, attachmentid, name, url in rows:
            record_count += 1
            file_name, extension = os.path.splitext(name or '')
            file_name = re.sub(r'[^a-zA-Z0-9]', '_', file_name) + extension
            target = f'{prefix}{attachmentid}{extension}'

            if blobs.pop(target, None) is None:
                _stream_to_gcs(url, bucket, target)
                yield debug(f"Transferred {file_name} to {target}")
            else:
                yield debug(f"Attachment {target} already exists in bucket {bucket_name}")

            cur.execute(update, [f'https://storage.googleapis.com/{bucket_name}/{target}',
                                 file_name, objectid])

        # Anything left was not in this run's attachment table.
        for blob in blobs.values():
            blob.delete()
            yield debug(f"Deleted attachment: {blob.name}")

    yield info(f"Finished transferring attachments to {bucket_name}")
    yield info(f"Total attachments transferred: {record_count}")
    yield info(f"Deleted {len(blobs)} attachments from bucket {bucket_name}")


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

def _truthy(value: Optional[str]) -> bool:
    return str(value or '').strip().lower() in ('true', '1', 'yes')


def parse_args() -> SimpleNamespace:
    """Parse and validate request parameters."""
    args = request.args if request.method == 'GET' else request.form
    retval = SimpleNamespace()

    retval.loglevel = args.get('loglevel', 'info')
    retval.service_name = security.service_name(args.get('service'))
    retval.url = security.layer_url(args.get('url'))
    retval.table = security.identifier(args.get('table'), 'table')
    retval.oid = security.identifier(args.get('oid', 'agol_to_pg_oid'), 'oid')
    retval.schema = security.identifier(args.get('schema', 'public'), 'schema')

    try:
        retval.batch = int(args.get('batch', '1000'))
    except ValueError as e:
        raise security.ValidationError('batch must be an integer') from e
    if not 1 <= retval.batch <= 10000:
        raise security.ValidationError('batch must be between 1 and 10000')

    retval.save_attachments = _truthy(args.get('save_attachments'))
    retval.bucket_name = args.get('bucket', os.getenv('BUCKET'))
    if retval.save_attachments:
        if not retval.bucket_name:
            raise security.ValidationError('Missing required parameter (bucket) for saving attachments')
        security.bucket_name(retval.bucket_name)

    geometry_name = args.get('geometry_name')
    retval.geometry_name = security.identifier(geometry_name, 'geometry_name') if geometry_name else None

    try:
        retval.source_epsg = int(args['source_epsg']) if args.get('source_epsg') else None
        retval.target_epsg = int(args['target_epsg']) if args.get('target_epsg') else None
    except ValueError as e:
        raise security.ValidationError('source_epsg and target_epsg must be integers') from e

    retval.metadata = _fetch_metadata(retval.url)
    _check_oid(retval.metadata, retval.oid)
    retval.has_geometry = bool(retval.metadata.get('geometryType'))
    if retval.has_geometry and not retval.source_epsg:
        retval.source_epsg = _source_epsg(retval.metadata)

    return retval


@agol_to_pg.app_errorhandler(ArcGISRequestError)
def _handle_arcgis_error(e):
    return Response(str(e), status=400)


@agol_to_pg.route('/agol2pg', methods=['GET', 'POST'])
def run_pg_script():
    """Run the AGOL to PostgreSQL sync.

    The response is streamed, so the HTTP status is 200 once streaming starts.
    The last line is always __EXIT_CODE__=0 on success or __EXIT_CODE__=1 on
    failure; clients must check it instead of the HTTP status.
    """
    args = parse_args()  # ValidationError / ArcGISRequestError become 400s
    if args.loglevel == 'debug':
        logger.setLevel(logging.DEBUG)

    def generator():
        exit_code = 0
        try:
            yield info("Starting AGOL to PostgreSQL script")
            yield debug({k: v for k, v in vars(args).items() if k != 'metadata'})
            setup_environment()

            # psycopg2's own context manager commits but does not close the connection.
            with contextlib.closing(connect(f"service={args.service_name}")) as conn:
                conn.autocommit = True

                if args.has_geometry and not args.source_epsg:
                    yield info("WARNING: No source_epsg was provided and it could not be "
                               "discovered from the service metadata.")

                yield from download_features(
                    conn=conn,
                    table=args.table,
                    schema=args.schema,
                    service_name=args.service_name,
                    url=args.url,
                    oid=args.oid,
                    batch_size=args.batch,
                    metadata=args.metadata,
                    geometry_name=args.geometry_name,
                    source_epsg=args.source_epsg,
                    target_epsg=args.target_epsg)

                if args.save_attachments:
                    yield info("Processing attachments as requested...")
                    yield from download_attachments(
                        conn, args.table, args.schema, args.service_name,
                        args.url, args.oid, args.batch, args.metadata)
                    yield from transfer_attachments(conn, args.table, args.schema, args.bucket_name)
                else:
                    yield info("Skipping attachments (save_attachments is false).")

            yield info("Finished AGOL to PostgreSQL script")
        except Exception as e:  # pylint: disable=broad-exception-caught
            exit_code = 1
            yield error(f"An error occurred: {e}")
            logger.error(traceback.format_exc())
        yield f"\n__EXIT_CODE__={exit_code}\n"

    return Response(stream_with_context(generator()), mimetype='text/event-stream')
