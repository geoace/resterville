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

import sys
import os
import traceback
from psycopg2 import sql, connect

# Set the PGSERVICEFILE environment variable to point to your PostgreSQL service file
def setup_environment():
    os.environ['PGSERVICEFILE'] = '/app/env/pg_service.conf'

def run_function(service_name, function_name, schema):
    """ Connects to the PostgreSQL database and runs the specified SQL function. """
    print(f"Connecting to PostgreSQL service: {service_name}")
    try:
        conn = connect(service=service_name)
        print("Connected")
        cursor = conn.cursor()
        try:
            # Set the search path to the specified schema
            cursor.execute(sql.SQL('SET search_path TO {schema}').format(schema=sql.Identifier(schema)))
            # Call the specified function
            cursor.execute(sql.SQL('SELECT {function}()').format(function=sql.Identifier(function_name)))
            conn.commit()
            print(f"Function {function_name} executed successfully in schema {schema}")
        except Exception as e:
            conn.rollback()
            print(f"Failed to execute function: {str(e)}", file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)
            sys.exit(1)
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        print(f"Failed to connect to PostgreSQL service: {str(e)}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: python pg_function.py <service_name> <function_name> <schema>", file=sys.stderr)
        sys.exit(1)
    service_name = sys.argv[1]
    function_name = sys.argv[2]
    schema = sys.argv[3]
    setup_environment()
    run_function(service_name, function_name, schema)
