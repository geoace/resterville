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
"""Convert Esri JSON feature sets to GeoJSON."""
import math
from typing import Optional


def _signed_area(ring) -> float:
    """Shoelace formula: positive for counter-clockwise rings, negative for clockwise."""
    area = 0.0
    for (x1, y1, *_), (x2, y2, *_) in zip(ring, ring[1:] + ring[:1]):
        area += x1 * y2 - x2 * y1
    return area / 2.0


def _point_in_ring(x: float, y: float, ring) -> bool:
    inside = False
    for (x1, y1, *_), (x2, y2, *_) in zip(ring, ring[1:] + ring[:1]):
        if (y1 > y) != (y2 > y):
            x_cross = x1 + (y - y1) * (x2 - x1) / (y2 - y1)
            if x < x_cross:
                inside = not inside
    return inside


def _rings_to_polygons(rings) -> list:
    """Group Esri rings into polygons of [outer, *holes].

    Esri outer rings are clockwise and holes are counter-clockwise. Every
    ring used to become its own polygon, which turned holes into filled
    polygons stacked on top of the shape they were cut out of.
    """
    rings = [r for r in rings if len(r) >= 4]
    if not rings:
        return []

    outers, holes = [], []
    for ring in rings:
        (outers if _signed_area(ring) <= 0 else holes).append(ring)

    if not outers:  # no clockwise rings: data does not follow Esri winding, treat all as shells
        return [[ring] for ring in rings]

    polygons = [[outer] for outer in outers]
    outer_areas = [abs(_signed_area(o)) for o in outers]
    for hole in holes:
        x, y = hole[0][0], hole[0][1]
        containing = [i for i, outer in enumerate(outers) if _point_in_ring(x, y, outer)]
        if containing:
            smallest = min(containing, key=lambda i: outer_areas[i])
            polygons[smallest].append(hole)
        else:
            # A counter-clockwise ring outside every shell is a shell drawn the wrong way.
            polygons.append([hole])
    return polygons


def _convert_geometry(geom: Optional[dict]) -> Optional[dict]:
    if not geom:
        return None

    if "x" in geom and "y" in geom:
        x, y = geom["x"], geom["y"]
        if x is None or y is None or (isinstance(x, float) and math.isnan(x)):
            return None
        return {"type": "Point", "coordinates": [x, y]}

    if geom.get("points"):
        return {"type": "MultiPoint", "coordinates": geom["points"]}

    if geom.get("paths"):
        paths = [p for p in geom["paths"] if len(p) >= 2]
        if not paths:
            return None
        if len(paths) == 1:
            return {"type": "LineString", "coordinates": paths[0]}
        return {"type": "MultiLineString", "coordinates": paths}

    if geom.get("rings"):
        polygons = _rings_to_polygons(geom["rings"])
        if not polygons:
            return None
        if len(polygons) == 1:
            return {"type": "Polygon", "coordinates": polygons[0]}
        return {"type": "MultiPolygon", "coordinates": polygons}

    # Empty geometry, or curve geometry (curvePaths/curveRings) which is not supported.
    return None


def esri_to_geojson(esri_json: dict, has_geometry: bool = True) -> dict:
    """Convert an Esri JSON query response into a GeoJSON FeatureCollection.

    Missing or empty geometries become null, which is valid GeoJSON. They used
    to be {"type": "", "coordinates": []}, which ogr2ogr rejects.
    """
    features = []
    for feature in esri_json.get("features", []):
        features.append({
            "type": "Feature",
            "properties": feature.get("attributes", {}),
            "geometry": _convert_geometry(feature.get("geometry")) if has_geometry else None,
        })
    return {"type": "FeatureCollection", "features": features}
