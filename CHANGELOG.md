# Changelog

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
