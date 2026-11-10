#!/bin/bash
set -uo pipefail

# Files written below hold credentials: keep them readable by this user only.
umask 077

ENV_DIR="/app/env"
MOUNTED_CREDENTIALS_PATH="/app/secrets/bucket-credentials"
mkdir -p "$ENV_DIR"

# ---------------------------------------------------------------------------
# Google credentials
# ---------------------------------------------------------------------------
if [ -f "$MOUNTED_CREDENTIALS_PATH" ]; then
    export GOOGLE_APPLICATION_CREDENTIALS="$MOUNTED_CREDENTIALS_PATH"
    echo "GOOGLE_APPLICATION_CREDENTIALS is set to $MOUNTED_CREDENTIALS_PATH"
elif [ -n "${GOOGLE_CREDENTIALS_JSON:-}" ]; then
    # GOOGLE_CREDENTIALS_JSON holds the key itself (local testing). Google
    # libraries expect a file path, so write it to a private file. It used to be
    # exported as the path directly, and validated with jq, which the image
    # does not include, so this branch never worked.
    CREDS_FILE="$ENV_DIR/google-credentials.json"
    if printf '%s' "$GOOGLE_CREDENTIALS_JSON" | python3 -c 'import json,sys; json.load(sys.stdin)' 2>/dev/null; then
        printf '%s' "$GOOGLE_CREDENTIALS_JSON" > "$CREDS_FILE"
        export GOOGLE_APPLICATION_CREDENTIALS="$CREDS_FILE"
        unset GOOGLE_CREDENTIALS_JSON
        echo "GOOGLE_APPLICATION_CREDENTIALS is set from GOOGLE_CREDENTIALS_JSON"
    else
        echo "Warning: GOOGLE_CREDENTIALS_JSON is set but is not valid JSON."
    fi
elif [ -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]; then
    echo "Using GOOGLE_APPLICATION_CREDENTIALS from the environment."
else
    echo "No explicit Google credentials; using the runtime's default service account."
fi

# ---------------------------------------------------------------------------
# pg_service.conf from PG_CONNECTION
# ---------------------------------------------------------------------------
# PG_CONNECTION is a semicolon-separated list of libpq connection strings:
#   dbname='db' user='me' host='h' port='5432' password='pw' [sslmode='require']
# Each becomes a service named <dbname>_<user>.
PG_SERVICE_CONF="${PGSERVICEFILE:-$ENV_DIR/pg_service.conf}"
: > "$PG_SERVICE_CONF"

extract() {
    # $1 = key, $2 = connection string
    printf '%s' "$2" | sed -nE "s/.*(^|[[:space:]])$1='([^']*)'.*/\2/p"
}

IFS=';' read -ra CONNECTIONS <<< "${PG_CONNECTION:-}"
for connection in "${CONNECTIONS[@]}"; do
    [ -z "${connection// /}" ] && continue

    dbname=$(extract dbname "$connection")
    user=$(extract user "$connection")
    host=$(extract host "$connection")
    port=$(extract port "$connection")
    password=$(extract password "$connection")
    sslmode=$(extract sslmode "$connection")

    if [ -z "$dbname" ] || [ -z "$user" ] || [ -z "$host" ]; then
        echo "Warning: skipping a PG_CONNECTION entry without dbname, user and host."
        continue
    fi

    {
        echo "[${dbname}_${user}]"
        echo "dbname=$dbname"
        echo "host=$host"
        echo "port=${port:-5432}"
        echo "user=$user"
        echo "password=$password"
        [ -n "$sslmode" ] && echo "sslmode=$sslmode"
        echo ""
    } >> "$PG_SERVICE_CONF"
    echo "Added PostgreSQL service ${dbname}_${user} (host $host)"
done
unset PG_CONNECTION
export PGSERVICEFILE="$PG_SERVICE_CONF"

# Activate the virtual environment
source /venv/bin/activate

exec "$@"
