"""
This file is part of RESTerville.

RESTerville is licensed under the GNU General Public License v3.0.
See the LICENSE file for more information.

This file uses the ArcGIS API for Python, which is licensed under the Apache License 2.0.
See https://github.com/Esri/arcgis-python-api/blob/master/LICENSE for more information.

"""
import psycopg2
import requests
import json
import argparse
import os
from datetime import datetime
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from arcgis.features import FeatureLayerCollection

client_id = os.getenv('ARCGIS_CLIENT_ID')
client_secret = os.getenv('ARCGIS_CLIENT_SECRET')

# Mapping PostgreSQL types to Esri field types
PG_TO_ESRI_TYPE_MAP = {
    'integer': 'esriFieldTypeInteger',
    'bigint': 'esriFieldTypeInteger',
    'smallint': 'esriFieldTypeSmallInteger',
    'text': 'esriFieldTypeString',
    'varchar': 'esriFieldTypeString',
    'character varying': 'esriFieldTypeString',
    'date': 'esriFieldTypeDate',
    'timestamp without time zone': 'esriFieldTypeDate',
    'timestamp with time zone': 'esriFieldTypeDate',
    'numeric': 'esriFieldTypeDouble',
    'double precision': 'esriFieldTypeDouble',
    'float': 'esriFieldTypeDouble'
    # Add additional mappings if necessary
}

def setup_environment():
    os.environ['PGSERVICEFILE'] = '/app/env/pg_service.conf'

def get_primary_key_column(service_name, schema, table):
    """Retrieve the primary key column name from a PostgreSQL table."""
    connection = psycopg2.connect(f"service={service_name}")
    cursor = connection.cursor()

    query = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        WHERE tc.constraint_type = 'PRIMARY KEY' AND
              tc.table_schema = %s AND
              tc.table_name = %s;
    """
    cursor.execute(query, (schema, table))
    primary_key = cursor.fetchone()
    cursor.close()
    connection.close()

    return primary_key[0] if primary_key else None


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for handling datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


def convert_geojson_to_esri_geometry(geojson_geom):
    if isinstance(geojson_geom, str):
        geojson_dict = json.loads(geojson_geom)
    elif isinstance(geojson_geom, dict):
        geojson_dict = geojson_geom
    else:
        raise TypeError("Expected geojson_geom to be a str or dict")

    geom_type = geojson_dict['type'].upper()
    esri_geom = {}

    if geom_type == 'POINT':
        esri_geom['x'] = geojson_dict['coordinates'][0]
        esri_geom['y'] = geojson_dict['coordinates'][1]
    elif geom_type == 'MULTIPOINT':
        esri_geom['points'] = geojson_dict['coordinates']
    elif geom_type == 'LINESTRING':
        esri_geom['paths'] = [geojson_dict['coordinates']]
    elif geom_type == 'MULTILINESTRING':
        esri_geom['paths'] = geojson_dict['coordinates']
    elif geom_type == 'POLYGON':
        esri_geom['rings'] = geojson_dict['coordinates']
    elif geom_type == 'MULTIPOLYGON':
        esri_geom['rings'] = [ring for polygon in geojson_dict['coordinates'] for ring in polygon]
    else:
        raise ValueError(f"Unsupported geometry type: {geom_type}")

    return esri_geom


def fetch_field_definitions(service_name, schema, table, geom='geom', ignore=None):
    """Fetch PostgreSQL table field definitions and map them to Esri field definitions."""
    connection = psycopg2.connect(f"service={service_name}")
    cursor = connection.cursor()

    # Fetch the primary key
    cursor.execute("""
        SELECT kcu.column_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        WHERE tc.constraint_type = 'PRIMARY KEY' AND
              tc.table_schema = %s AND
              tc.table_name = %s;
    """, (schema, table))
    primary_key = cursor.fetchone()
    primary_key_column = primary_key[0] if primary_key else None

    # Prepare the ignore set, ensuring no ObjectID is included
    ignore_set = set(ignore.split(',')) if ignore else set()
    ignore_set.update([geom])  # Ignore geometry and other redundant fields

    query = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND column_name NOT IN %s;
    """
    cursor.execute(query, (schema, table, tuple(ignore_set)))
    fields = cursor.fetchall()
    cursor.close()
    connection.close()

    esri_fields = []
    for column_name, data_type in fields:
        esri_type = 'esriFieldTypeOID' if column_name == primary_key_column else PG_TO_ESRI_TYPE_MAP.get(data_type, 'esriFieldTypeString')
        esri_fields.append({"name": column_name, "type": esri_type, "alias": column_name})

    return esri_fields

# Ensure that the appending process does not attempt to insert ObjectID
def prepare_features(raw_features, ignore_fields=None):
    if ignore_fields is None:
        ignore_fields = []

    prepared_features = []
    for item in raw_features:
        attributes = {k: v for k, v in item['attributes'].items() if k.lower() != 'objectid' and k not in ignore_fields}
        
        # Convert datetime objects to ISO format
        for k, v in attributes.items():
            if isinstance(v, datetime):
                attributes[k] = v.isoformat()

        prepared_features.append({"attributes": attributes, "geometry": item['geometry']})
    return prepared_features



def get_token(portal_url):
    """Authenticate with ArcGIS and return a token using client credentials."""
    client_id = os.getenv('ARCGIS_CLIENT_ID')
    client_secret = os.getenv('ARCGIS_CLIENT_SECRET')

    url = f"{portal_url}/sharing/rest/oauth2/token/"
    data = {
        'f': 'json',
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials'
    }
    response = requests.post(url, data=data)
    response_json = response.json()
    if 'error' in response_json:
        print("Error obtaining token:", response_json['error'])
        return None
    return response_json.get("access_token")

def fetch_data_from_postgis(service_name, schema, table, geom='geom', ignore=None, target_epsg='3857'):
    connection = psycopg2.connect(f"service={service_name}")
    cursor = connection.cursor()

    cursor.execute(f"SELECT GeometryType({geom}) FROM {schema}.{table} LIMIT 1;")
    geom_type = cursor.fetchone()[0]

    ignore_set = set(ignore.split(',')) if ignore else set()
    ignore_set.add(geom)  # Ignore the geometry column in attribute fetch

    cursor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = %s AND table_name = %s AND column_name NOT IN %s", (schema, table, tuple(ignore_set)))
    columns = cursor.fetchall()
    columns_sql = ', '.join([col[0] for col in columns if col[0] != geom])

    query = f"SELECT {columns_sql}, ST_AsGeoJSON(ST_Transform({geom}, {int(target_epsg)})) as geom FROM {schema}.{table};"
    cursor.execute(query)
    data = cursor.fetchall()

    features = []
    for record in data:
        attributes = {col[0]: rec for col, rec in zip(columns, record[:-1])}
        esri_geom = convert_geojson_to_esri_geometry(record[-1])  # Convert GeoJSON to Esri format

        features.append({"attributes": attributes, "geometry": esri_geom})
    cursor.close()
    connection.close()

    return features




def setup_environment():
    # Set the PGSERVICEFILE environment variable
    os.environ['PGSERVICEFILE'] = '/app/env/pg_service.conf'

def get_feature_layer(url, token):
    """Authenticate and retrieve a feature layer."""
    gis = GIS("https://www.arcgis.com", token=token)
    feature_layer = FeatureLayer(url, gis)
    return feature_layer

def delete_all_features(feature_layer):
    """Delete all features in the feature layer."""
    try:
        delete_result = feature_layer.delete_features(where="1=1")  # Deletes all features
        return delete_result
    except Exception as e:
        print(f"Failed to delete features: {str(e)}")
        return None

def append_features(feature_layer, features, batch_size=100):
    """Append features to a feature layer using the ArcGIS API for Python."""
    total_features = len(features)
    add_results = []

    for start in range(0, total_features, batch_size):
        end = start + batch_size
        batch_features = features[start:end]

        try:
            # Send the batch features as a dictionary directly
            response = feature_layer.edit_features(adds=batch_features)
            add_results.append(response)
            print(f"Successfully added features {start} to {end} of {total_features}")
        except Exception as e:
            # Print details of the batch and exception for debugging
            print(f"Failed to add features {start} to {end} of {total_features}: {str(e)}")

    return add_results


def get_postgis_srid(service_name, schema, table, geom='geom'):
    """Fetch the SRID of the specified geometry column from the PostgreSQL table."""
    connection = psycopg2.connect(f"service={service_name}")
    cursor = connection.cursor()

    # Query the SRID directly from the spatial column
    cursor.execute(f"SELECT Find_SRID('{schema}', '{table}', '{geom}')")
    srid = cursor.fetchone()[0]

    cursor.close()
    connection.close()

    return srid


def get_postgis_geometry_type(service_name, schema, table, geom='geom'):
    """Fetch the geometry type from a PostgreSQL table."""
    connection = psycopg2.connect(f"service={service_name}")
    cursor = connection.cursor()
    
    cursor.execute(f"SELECT GeometryType({geom}) FROM {schema}.{table} LIMIT 1;")
    geom_type = cursor.fetchone()[0].upper()
    cursor.close()
    connection.close()

    # Map to Esri geometry types
    geometry_type_mapping = {
        'POINT': 'esriGeometryPoint',
        'MULTIPOINT': 'esriGeometryMultipoint',
        'LINESTRING': 'esriGeometryPolyline',
        'MULTILINESTRING': 'esriGeometryPolyline',
        'POLYGON': 'esriGeometryPolygon',
        'MULTIPOLYGON': 'esriGeometryPolygon'
        # Add additional mappings if required
    }

    return geometry_type_mapping.get(geom_type, 'esriGeometryPolygon')  # Default to polygon


def get_or_create_new_feature_service(token, service_name, schema, table, geom='geom', ignore=None):
    """Create a new feature service dynamically using field definitions from PostgreSQL."""
    portal_url = os.getenv('ARCGIS_PORTAL_URL')

    # Attempt to authenticate using the token
    gis = GIS(portal_url, token=token)
    if not gis.users.me:
        # If the token-based authentication fails, fall back to username and password
        username = os.getenv('ARCGIS_USER')
        password = os.getenv('ARCGIS_PASSWORD')
        if not username or not password:
            print("Username or password is missing from environment variables.")
            return None

        print("Falling back to username and password authentication.")
        gis = GIS(portal_url, username, password)

    if not gis.users.me:
        print("Authentication failed.")
        return None

    # Title and description of the new feature service
    name = f"{table}_Feature_Service"
    description = f"A feature service containing data from {schema}.{table}"

    # Retrieve field definitions from PostgreSQL, excluding geom and ignore fields
    esri_fields = fetch_field_definitions(service_name, schema, table, geom, ignore)

    # Determine the geometry type dynamically
    geometry_type = get_postgis_geometry_type(service_name, schema, table, geom)

    # Create a new feature service with editing capabilities
    new_service = gis.content.create_service(
        name=name,
        service_type='featureService',
        description=description,
        capabilities="Create, Query, Update, Delete"  # Enable all required editing operations
    )

    # Add the layer definitions to the service
    collection = FeatureLayerCollection.fromitem(new_service)
    collection.manager.add_to_definition({
        "layers": [{
            "name": table,
            "geometryType": geometry_type,
            "fields": esri_fields
        }]
    })

    return collection.layers[0].url

def delete_all_features(feature_layer):
    try:
        delete_result = feature_layer.delete_features(where="1=1")  # Deletes all features
        return delete_result
    except Exception as e:
        print(f"Failed to delete features: {str(e)}")
        return None


def main():
    parser = argparse.ArgumentParser(description='Transfer data from PostGIS to ArcGIS Online.')
    parser.add_argument('service_name', help='PostgreSQL service name for connection')
    parser.add_argument('url', help='ArcGIS service URL on which to append data')
    parser.add_argument('table', help='PostgreSQL table name to source data')
    parser.add_argument('--schema', default='public', help='Schema of the PostgreSQL table')
    parser.add_argument('--geom', default='geom', help='Geometry column name')
    parser.add_argument('--ignore', default='', help='Comma-separated list of fields to ignore')
    parser.add_argument('--portal_url', default='https://www.arcgis.com', help='ArcGIS Online portal URL')
    parser.add_argument('--batch', type=int, default=100, help='Batch size for feature appending')
    parser.add_argument('--truncate', default='no', choices=['yes', 'no'], help='Whether to delete all existing features before appending')
    parser.add_argument('--target_epsg', default='3857', help='Target EPSG code for geometry transformation')

    args = parser.parse_args()


    ignore_fields = [field.strip() for field in args.ignore.split(',') if field.strip()]

    token = get_token(args.portal_url)
    if not token:
        print("Failed to obtain token.")
        return

    setup_environment()


    url = args.url

    feature_layer = get_feature_layer(url, token)

    if args.truncate.lower() == 'yes':
        print("Deleting all existing features...")
        delete_all_features(feature_layer)
    else: 
        print("No truncate detected. Appending features without deletion of old.")

    features = fetch_data_from_postgis(args.service_name, args.schema, args.table, args.geom, args.ignore, args.target_epsg)
    prepared_features = prepare_features(features, ignore_fields)

    results = append_features(feature_layer, prepared_features, args.batch)
    # print(results)

    # for feature in prepared_features:
    #     # Print geometries cleanly formatted as JSON
    #     print(json.dumps(feature, indent=4))


if __name__ == "__main__":
    main()
