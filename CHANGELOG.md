# Changelog

## - 2026-11-10

### Security
- API key: compared in constant time, accepted from the `X-API-Key` header (query/form `api_key` still works), and no longer written to the logs. Default log level is now INFO, since DEBUG made urllib3 log request URLs containing tokens and keys.
- SQL injection: every query built from request parameters (agol_to_pg attachment tables, transfer_attachments UPDATE, pg_to_agol reads) now uses `psycopg2.sql.Identifier` or bound parameters. `service`, `table`, `schema`, `oid`, `geometry_name` and `bucket` are validated.
- ArcGIS token leak: the token was sent to whatever host the `url` parameter (or an attachment URL) pointed at. It is now only sent over https to ArcGIS Online, `ARCGIS_PORTAL_URL`, or hosts in `ARCGIS_TOKEN_HOSTS`, and in a POST body rather than the URL.
- ogr2ogr no longer forces `sslmode=disable`; SSL follows pg_service.conf / `PGSSLMODE` (libpq default `prefer`). `PG_CONNECTION` entries may include `sslmode='require'`.
- Container runs as an unprivileged user under gunicorn instead of root under the Flask development server. pg_service.conf and credential files are written with mode 600.

### Docker image
- Pinned to a released GDAL image (`ubuntu-small-3.12.4`) instead of `ubuntu-small-latest`, which is a nightly build of GDAL master.
- `apt-get upgrade` at build time; removed unused packages (libkrb5-dev, wget, gnupg, lsb-release) and unused Python packages (GDAL bindings, dask, pyproj, pillow as direct dependencies).
- Python dependencies installed from a hashed `requirements.lock`.

### agol_to_pg
- Streamed responses end with `__EXIT_CODE__=0` or `__EXIT_CODE__=1`. The HTTP status is always 200 once streaming starts, so clients must check this line.
- Fixed the token retry, which discarded the retried response and returned the original error.
- ogr2ogr failures now include ogr2ogr's own error output.
- Paging uses `exceededTransferLimit` and orders by the ObjectID field, so services with a lower `maxRecordCount` than `batch` are no longer cut off after the first page.
- Geometry type comes from the layer definition instead of the first feature of each batch.
- Clear error when an existing table lacks the `oid` column (previously a foreign key error in the attachments step).
- Attachments: the temp table was never cleared between batches, so attachments were inserted more than once; fixed. Works on layers without GlobalIDs.
- transfer_attachments used the bucket prefix `table` without a slash, so syncing `hydrant` deleted blobs belonging to `hydrant_inspections`; fixed.
- Temp files are always removed, and live in the system temp directory.
- 400 responses for bad parameters instead of 500.

### esri_to_geojson
- Polygon holes were written as separate filled polygons. Rings are now grouped into shells and holes by Esri winding order.
- Null and empty geometries become `null` (valid GeoJSON) instead of `{"type": ""}`.

### pg_to_agol
- Honors `portal_url` for the feature layer; reports failed adds and exits non-zero; serializes `date` and `Decimal` values; exports polygons with Esri ring order (`ST_ForcePolygonCW`).

### backup
- Retention deleted the only backup of any item that had not changed within `duration` days. The newest backup of each layer is now always kept.
- Backup matching was by substring (`Parcel` matched `Parcel_Lines`) and listed the bucket once per item; now exact and listed once.
- Export items are deleted from the portal even when the upload fails.

### app
- `/pg2agol`, `/backup` and `/pg_service` could deadlock reading stdout and stderr alternately; output is now merged like `/pg_function`.
- `/backup` failed to import `lib.gcp` when run as a script; all helpers now run with `python -m`.
- Added unit tests (`tests/`).

## - 2025-04-24

### agol_to_pg
- Removed geometry_name as required when calling a feature service with geometry
- Updated no source_epsg from abort 400 to a warning message

## - 2025-04-23

### agol_to_pg
- Updated to handle tables without geometry
    - AGOL service is checked for a geometryType field before validating and using the geometry_name, source_epsg and target_epsg fields.

### esri_to_geojson
- Updated to handle no geometry

## - 2025-04-13

### agol_to_pg
- Added download attachments and store them in a Google cloud platform bucket
- Updated logic behind streaming messages during execution for better error handling
- Changes to support local testing using VSCode
