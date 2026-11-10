# This file is part of RESTerville, a Workflow Automation toolkit.
# Copyright (C) 2024  GEOACE
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""Input validation and outbound-request guards shared by the endpoints."""
import os
import re
from typing import Optional
from urllib.parse import urlparse, urlunparse

# Lowercase only: ogr2ogr launders table and column names to lowercase, so a
# mixed-case name would not match the table it creates once quoted in SQL.
_IDENT_RE = re.compile(r'^[a-z_][a-z0-9_]{0,62}$')
_SERVICE_RE = re.compile(r'^[A-Za-z0-9_.-]{1,128}$')
_BUCKET_RE = re.compile(r'^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$')

# Default timeout for outbound HTTP calls: (connect, read) in seconds.
HTTP_TIMEOUT = (10, int(os.getenv('HTTP_READ_TIMEOUT', '300')))


class ValidationError(ValueError):
    """Raised when a request parameter fails validation."""


def identifier(value: Optional[str], name: str) -> str:
    """Validate a PostgreSQL identifier (schema, table, column)."""
    if not value or not _IDENT_RE.match(value):
        raise ValidationError(
            f"Invalid {name} '{value}': use lowercase letters, digits and underscores, "
            "starting with a letter or underscore (max 63 characters).")
    return value


def service_name(value: Optional[str]) -> str:
    """Validate a pg_service.conf service name.

    The name is interpolated into libpq connection strings, so anything that
    could add extra connection keywords (spaces, '=', quotes) is rejected.
    """
    if not value or not _SERVICE_RE.match(value):
        raise ValidationError(f"Invalid service name '{value}'.")
    return value


def bucket_name(value: Optional[str]) -> str:
    """Validate a Google Cloud Storage bucket name."""
    if not value or not _BUCKET_RE.match(value):
        raise ValidationError(f"Invalid bucket name '{value}'.")
    return value


def layer_url(value: Optional[str]) -> str:
    """Validate an ArcGIS layer URL and return it without query string or trailing slash."""
    if not value:
        raise ValidationError("Missing url.")
    parsed = urlparse(value.strip())
    if parsed.scheme != 'https' or not parsed.hostname:
        raise ValidationError(f"Invalid url '{value}': an https URL is required.")
    path = parsed.path.rstrip('/')
    return urlunparse((parsed.scheme, parsed.netloc, path, '', '', ''))


def _token_hosts() -> set:
    hosts = set()
    portal = os.getenv('ARCGIS_PORTAL_URL')
    if portal:
        host = urlparse(portal if '://' in portal else f'https://{portal}').hostname
        if host:
            hosts.add(host.lower())
    for extra in os.getenv('ARCGIS_TOKEN_HOSTS', '').split(','):
        extra = extra.strip().lower()
        if extra:
            hosts.add(extra)
    return hosts


def token_allowed(url: str) -> bool:
    """Return True if an ArcGIS token may be sent to this URL.

    Tokens are only sent over https to ArcGIS Online, the configured portal,
    or hosts listed in ARCGIS_TOKEN_HOSTS. Without this check, any caller with
    the API key could make the server send its ArcGIS token to a host they
    control by passing that host as the layer or attachment URL.
    """
    parsed = urlparse(url)
    host = (parsed.hostname or '').lower()
    if parsed.scheme != 'https' or not host:
        return False
    if host == 'arcgis.com' or host.endswith('.arcgis.com'):
        return True
    return host in _token_hosts()
