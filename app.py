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

import hmac
import logging
import os
import subprocess
import sys

from flask import Flask, Response, abort, render_template, request, stream_with_context

from lib import security
from lib.agol_to_pg import agol_to_pg

APP_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()


class FlushHandler(logging.StreamHandler):
    """Stream handler that flushes after every record so Cloud Run sees logs immediately."""

    def emit(self, record):
        super().emit(record)
        self.flush()


# INFO by default. At DEBUG, urllib3 logs full request URLs, which include
# ArcGIS tokens and API keys passed as query parameters.
_handler = FlushHandler(sys.stdout)
_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.basicConfig(level=LOG_LEVEL, handlers=[_handler], force=True)

API_KEY = os.getenv('API_KEY')
if not API_KEY:
    logging.error('API_KEY not set in environment variables')
    sys.exit(1)
if len(API_KEY) < 20:
    logging.warning('API_KEY is shorter than 20 characters; use a long random value.')

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 1 * 1024 * 1024
app.register_blueprint(agol_to_pg)


def _param(name, default=None):
    """Read a parameter from the query string (GET) or form body (POST)."""
    source = request.args if request.method == 'GET' else request.form
    return source.get(name, default)


def _supplied_api_key():
    """Prefer the X-API-Key header; fall back to api_key in the query or form for older clients."""
    return (request.headers.get('X-API-Key')
            or request.args.get('api_key')
            or request.form.get('api_key'))


@app.before_request
def validate_api_key():
    """Reject requests whose API key does not match."""
    supplied = _supplied_api_key() or ''
    if not hmac.compare_digest(supplied.encode(), API_KEY.encode()):
        abort(403)


def _stream_command(command, label):
    """Run a command and stream its combined stdout/stderr as server-sent events.

    stderr is merged into stdout. Reading the two pipes alternately with
    readline() can deadlock once the unread pipe fills its buffer.
    """
    logging.info("Running %s: %s", label, ' '.join(command[1:]))

    def generate():
        process = subprocess.Popen(
            command,
            cwd=APP_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        try:
            for line in iter(process.stdout.readline, ''):
                line = line.rstrip()
                print(f"[{label}] {line}", flush=True)
                yield f"data:{line}\n\n"
            rc = process.wait()
            print(f"[{label}] exit code={rc}", flush=True)
            yield f"data:__EXIT_CODE__={rc}\n\n"
        finally:
            if process.poll() is None:
                process.kill()

    return Response(stream_with_context(generate()), mimetype='text/event-stream')


@app.errorhandler(security.ValidationError)
def handle_validation_error(e):
    return Response(str(e), status=400)


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/pg2agol', methods=['GET', 'POST'])
def run_pg_to_agol_script():
    service_name = security.service_name(_param('service'))
    url = security.layer_url(_param('url'))
    table = _param('table')
    schema = _param('schema', 'public')
    batch = _param('batch', '100')
    truncate = _param('truncate', 'no')
    target_epsg = _param('target_epsg', '3857')
    geom = _param('geom')
    ignore = _param('ignore')
    portal_url = _param('portal_url')

    if not table:
        return Response('Missing required parameters (service, url, table)', status=400)
    if any(v and v.startswith('-') for v in (table, schema, geom, ignore)):
        return Response('Parameter values may not start with "-"', status=400)
    if not batch.isdigit() or not target_epsg.isdigit():
        return Response('batch and target_epsg must be integers', status=400)
    if truncate not in ('yes', 'no'):
        return Response("truncate must be 'yes' or 'no'", status=400)

    command = [sys.executable, '-m', 'lib.pg_to_agol', service_name, url, table,
               '--schema', schema, '--batch', batch, '--truncate', truncate,
               '--target_epsg', target_epsg]
    if geom:
        command += ['--geom', geom]
    if ignore:
        command += ['--ignore', ignore]
    if portal_url:
        command += ['--portal_url', security.layer_url(portal_url)]

    return _stream_command(command, 'pg2agol')


@app.route('/backup', methods=['GET', 'POST'])
def backup():
    remove_archives = _param('remove_archives', 'no')
    duration = _param('duration')
    bucket = _param('bucket', os.getenv('BUCKET'))
    usernames = _param('usernames')

    if not usernames:
        return Response("The 'usernames' parameter is required", status=400)
    if remove_archives not in ('yes', 'no'):
        return Response("remove_archives must be 'yes' or 'no'", status=400)
    if duration and not duration.isdigit():
        return Response('duration must be an integer number of days', status=400)

    command = [sys.executable, '-m', 'lib.backup', '--remove_archives',
               remove_archives, '--usernames', usernames]
    if remove_archives == 'yes' and duration:
        command += ['--duration', duration]
    if bucket:
        command += ['--bucket_name', security.bucket_name(bucket)]

    return _stream_command(command, 'backup')


@app.route('/pg_function', methods=['GET', 'POST'])
def pg_function():
    service_name = security.service_name(_param('service', 'default_service'))
    function_name = _param('function')
    schema = _param('schema', 'public')

    # function_name and schema are quoted with sql.Identifier in lib/pg_function.py.
    if not function_name:
        return Response("Function parameter is required", status=400)

    command = [sys.executable, '-m', 'lib.pg_function', service_name, function_name, schema]
    return _stream_command(command, 'pg_function')


@app.route('/pg_service', methods=['GET', 'POST'])
def pg_service():
    return _stream_command([sys.executable, '-m', 'lib.get_services'], 'pg_service')


if __name__ == '__main__':
    # Local development only. The container runs gunicorn (see Dockerfile).
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', '8080')))
