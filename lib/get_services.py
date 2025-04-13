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
import logging
import re


def get_pg_services(pg_service_conf_path="/app/env/pg_service.conf"):
    """Retrieve the PostgreSQL service names from the pg_service.conf file."""
    service_names = []

    try:
        # Read the pg_service.conf file and extract the service names (section headers)
        with open(pg_service_conf_path, 'r') as f:
            for line in f:
                match = re.match(r'^\[([^\]]+)\]$', line.strip())
                if match:
                    service_names.append(match.group(1))

    except FileNotFoundError:
        logging.error(f"pg_service.conf file not found at {pg_service_conf_path}")
        return {"error": "pg_service.conf file not found"}
    except Exception as e:
        logging.error(f"Error reading pg_service.conf: {e}")
        return {"error": str(e)}

    return {"services": service_names}

if __name__ == "__main__":
    # Execute and return the output as JSON for the subprocess to capture
    result = get_pg_services()
    print(json.dumps(result))
