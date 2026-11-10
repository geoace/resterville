# Pin a released GDAL image. "ubuntu-small-latest" is rebuilt nightly from GDAL
# master, so every build pulled a different, unreleased GDAL plus whatever OS
# packages were current that day.
ARG GDAL_VERSION=3.12.4
FROM ghcr.io/osgeo/gdal:ubuntu-small-${GDAL_VERSION}

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Apply OS security updates, then add only what the app needs. The base image
# already ships python3. libkrb5-dev, wget, gnupg and lsb-release were unused.
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends python3-venv ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /venv \
    && /venv/bin/pip install --upgrade pip setuptools wheel

WORKDIR /app

# Pinned and hash-checked dependencies (see requirements.txt for how to regenerate).
COPY requirements.lock /app/requirements.lock
RUN /venv/bin/pip install --require-hashes -r /app/requirements.lock

COPY . /app

# Run as an unprivileged user. /app/env is written at startup (pg_service.conf).
RUN useradd --system --uid 10001 --home-dir /app --shell /usr/sbin/nologin resterville \
    && mkdir -p /app/env \
    && chown -R resterville:resterville /app/env \
    && chmod 0755 /app/entrypoint.sh

USER resterville

ENV PATH="/venv/bin:${PATH}" \
    PGSERVICEFILE=/app/env/pg_service.conf

EXPOSE 8080
ENTRYPOINT ["/app/entrypoint.sh"]

# gunicorn binds to $PORT (Cloud Run sets it; defaults to 8080 here).
# --timeout 0: syncs stream for a long time; Cloud Run's request timeout is the limit.
# No access log: request URLs can carry api_key in the query string.
CMD ["sh", "-c", "exec gunicorn --bind 0.0.0.0:${PORT:-8080} --workers 1 --threads 8 --timeout 0 app:app"]
