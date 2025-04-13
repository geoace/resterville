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

import logging

import psycopg2
from psycopg2 import sql

# Configure logging
logging.basicConfig(level=logging.INFO)


def truncate_or_delete_table(table_name, service_name, schema='public', cascade: bool = False, reset_sequence: bool = False):
    """
    Truncates the specified table in the PostgreSQL database using a service definition.
    If truncating fails (e.g., due to foreign key constraints), delete all rows instead.

    Args:
    table_name (str): The name of the table to truncate or delete from.
    service_name (str): The PostgreSQL service name as defined in pg_service.conf.
    schema (str): The database schema in which the table resides. Default is 'public'.
    """

    # Establish a connection using the service name
    with psycopg2.connect(f"service={service_name}") as conn:
        conn.autocommit = True  # Enable autocommit for DDL commands like TRUNCATE
        try:
            with conn.cursor() as cur:
                # Construct the TRUNCATE SQL query
                query = sql.SQL("TRUNCATE TABLE {}.{}{}{}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table_name),
                    sql.SQL(" RESTART IDENTITY" if reset_sequence else ""),
                    sql.SQL(" CASCADE" if cascade else "")
                )

                # Execute the truncate operation
                cur.execute(query)
                logging.info(
                    "Table %s.%s truncated successfully.", schema, table_name)
        except psycopg2.errors.FeatureNotSupported as e:
            if "cannot truncate a table referenced in a foreign key constraint" in str(e):
                # If an error occurs during truncation, log the error and attempt to delete instead
                logging.warning(
                    """Failed to truncate table %s.%s, perhaps because of a dependency within the database. 
                    Trying to delete features without truncating. Error: %s""", schema, table_name, str(e))

                try:
                    with conn.cursor() as cur:
                        # Construct the DELETE SQL query
                        delete_query = sql.SQL("DELETE FROM {}.{}").format(
                            sql.Identifier(schema),
                            sql.Identifier(table_name)
                        )

                        # Execute the delete operation
                        cur.execute(delete_query)
                        logging.info(
                            "All features in table %s.%s deleted successfully.", schema, table_name)

                        if reset_sequence:
                            logging.warning(
                                "Resetting sequence after deletion is not currently supported.")
                except Exception as delete_error:
                    logging.error(
                        "Failed to delete features in table %s.%s. Error: %s",
                        schema, table_name, str(delete_error))
                    raise delete_error
            else:
                raise e
        except Exception as e:
            logging.error(
                "Failed to truncate features in table %s.%s. Error: %s", schema, table_name, str(e))
            raise e
