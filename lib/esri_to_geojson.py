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


def esri_to_geojson(esri_json):
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }

    for feature in esri_json.get("features", []):
        geo_feature = {
            "type": "Feature",
            "properties": feature.get("attributes", {}),
            "geometry": {
                "type": "",
                "coordinates": []
            }
        }

        # Handle different geometry types from ESRI JSON
        if feature.get("geometry"):
            geom = feature["geometry"]

            # Check if it's a Point
            if "x" in geom and "y" in geom:
                geo_feature["geometry"]["type"] = "Point"
                geo_feature["geometry"]["coordinates"] = [geom["x"], geom["y"]]

            # Check if it's a MultiPoint
            elif "points" in geom:
                geo_feature["geometry"]["type"] = "MultiPoint"
                geo_feature["geometry"]["coordinates"] = geom["points"]

            # Check if it's a LineString or MultiLineString
            elif "paths" in geom:
                if len(geom["paths"]) == 1:
                    geo_feature["geometry"]["type"] = "LineString"
                    geo_feature["geometry"]["coordinates"] = geom["paths"][0]
                else:
                    geo_feature["geometry"]["type"] = "MultiLineString"
                    geo_feature["geometry"]["coordinates"] = geom["paths"]

            # Check if it's a Polygon or MultiPolygon
            elif "rings" in geom:
                if len(geom["rings"]) == 1:
                    geo_feature["geometry"]["type"] = "Polygon"
                    geo_feature["geometry"]["coordinates"] = geom["rings"]
                else:
                    geo_feature["geometry"]["type"] = "MultiPolygon"
                    polygons = [[ring] for ring in geom["rings"]]
                    geo_feature["geometry"]["coordinates"] = polygons

        geojson["features"].append(geo_feature)

    return geojson
