#!/bin/bash

# Set the path to the mounted credentials file
MOUNTED_CREDENTIALS_PATH="/app/secrets/bucket-credentials"

# Function to validate JSON
validate_json() {
    local json_str="$1"
    echo "$json_str" | jq . > /dev/null 2>&1
    return $?
}

# Check if the mounted credentials file exists
if [ -f "$MOUNTED_CREDENTIALS_PATH" ]; then
    export GOOGLE_APPLICATION_CREDENTIALS="$MOUNTED_CREDENTIALS_PATH"
    echo "GOOGLE_APPLICATION_CREDENTIALS is set to $MOUNTED_CREDENTIALS_PATH"
elif [ -n "$GOOGLE_CREDENTIALS_JSON" ]; then
    # If GOOGLE_CREDENTIALS_JSON is set (for local testing), validate it
    if validate_json "$GOOGLE_CREDENTIALS_JSON"; then
        export GOOGLE_APPLICATION_CREDENTIALS="$GOOGLE_CREDENTIALS_JSON"
        echo "GOOGLE_APPLICATION_CREDENTIALS is set to the path provided in GOOGLE_CREDENTIALS_JSON"
    else
        echo "Warning: GOOGLE_CREDENTIALS_JSON is set but is not valid JSON."
    fi
else
    echo "Warning: Neither GOOGLE_APPLICATION_CREDENTIALS nor GOOGLE_CREDENTIALS_JSON is set."
    echo "Proceeding without explicit credentials. The application should handle credentials internally."
fi

# Initialize or overwrite the pg_service.conf file
PG_SERVICE_CONF="/app/env/pg_service.conf"  # Adjust the path as needed
echo -n "" > "$PG_SERVICE_CONF"

# Function to add a service to the pg_service.conf file
add_pg_service() {
    dbname="$1"
    user="$2"
    host="$3"
    port="$4"
    password="$5"
    service_name="${dbname}_${user}"  # Use dbname_username format for the service name

    {
        echo "[$service_name]"
        echo "dbname=$dbname"
        echo "host=$host"
        echo "port=$port"
        echo "user=$user"
        echo "password=$password"
        echo ""  # Blank line between services
    } >> "$PG_SERVICE_CONF"
}

# Split the PG_CONNECTION string based on semicolons
IFS=';' read -ra CONNECTIONS <<< "$PG_CONNECTION"

for connection in "${CONNECTIONS[@]}"; do
    # Extract fields from the psycopg connection string format using regex
    dbname=$(echo "$connection" | sed -nE "s/.*dbname='([^']+)'.*/\1/p")
    user=$(echo "$connection" | sed -nE "s/.*user='([^']+)'.*/\1/p")
    host=$(echo "$connection" | sed -nE "s/.*host='([^']+)'.*/\1/p")
    port=$(echo "$connection" | sed -nE "s/.*port='([^']+)'.*/\1/p")
    password=$(echo "$connection" | sed -nE "s/.*password='([^']+)'.*/\1/p")

    add_pg_service "$dbname" "$user" "$host" "$port" "$password"
done

# Activate the virtual environment
source /venv/bin/activate

# Run the main command
exec "$@"
