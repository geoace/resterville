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
import os
import subprocess
import sys
import traceback

from flask import (Flask, Response, abort, render_template, request,
                   stream_with_context)

from lib.agol_to_pg import agol_to_pg

# Configure logging to output to stdout immediately
logging.basicConfig(
    level=logging.DEBUG,  # Adjust the log level as needed
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Ensure real-time flushing


class FlushHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()


# Apply the flush handler to the root logger
logging.getLogger().handlers = [FlushHandler(sys.stdout)]

API_KEY = os.getenv('API_KEY')
if not API_KEY:
    logging.error('API_KEY not set in environment variables')
    sys.exit(1)

app = Flask(__name__)
app.register_blueprint(agol_to_pg)


@app.before_request
def validate_api_key():
    """ Validate that the API key in the request arguments matches the expected API key. """
    api_key = request.args.get(
        'api_key') if request.method == 'GET' else request.form.get('api_key')

    logging.debug(f"Received API key: {api_key}")  # Debug print
    if api_key != API_KEY:
        abort(403)


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/pg2agol', methods=['GET', 'POST'])
def run_pg_to_agol_script():
    try:
        # Fetch parameters based on the request method
        if request.method == 'GET':
            service_name = request.args.get('service')
            url = request.args.get('url')
            table = request.args.get('table')
            schema = request.args.get('schema', 'public')
            # Set default batch size to 100
            batch = request.args.get('batch', '100')
            # Default option for truncation
            truncate = request.args.get('truncate', 'no')
            target_epsg = request.args.get(
                'target_epsg', '3857')  # Default EPSG code
            geom = request.args.get('geom')
            ignore = request.args.get('ignore')
            portal_url = request.args.get('portal_url')
        elif request.method == 'POST':
            service_name = request.form.get('service')
            url = request.form.get('url')
            table = request.form.get('table')
            schema = request.form.get('schema', 'public')
            # Set default batch size to 100
            batch = request.form.get('batch', '100')
            # Default option for truncation
            truncate = request.form.get('truncate', 'no')
            target_epsg = request.form.get(
                'target_epsg', '3857')  # Default EPSG code
            geom = request.form.get('geom')
            ignore = request.form.get('ignore')
            portal_url = request.form.get('portal_url')

        if not service_name or not url or not table:
            return Response('Missing required parameters (service, url, table)', status=400)

        # Construct the command line arguments
        command = ['python3', 'lib/pg_to_agol.py', service_name, url, table, '--schema',
                   schema, '--batch', batch, '--truncate', truncate, '--target_epsg', target_epsg]

        # Optional parameters with command line handling
        if geom:
            command.extend(['--geom', geom])
        if ignore:
            command.extend(['--ignore', ignore])
        if portal_url:
            command.extend(['--portal_url', portal_url])

        print(f"Running command: {' '.join(command)}")  # Debug print

        def generate():
            process = subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

            while True:
                output = process.stdout.readline()
                if output:
                    yield f"data:{output}\n\n"
                err = process.stderr.readline()
                if err:
                    yield f"data:{err}\n\n"
                if output == '' and process.poll() is not None:
                    break

        return Response(stream_with_context(generate()), mimetype='text/event-stream')

    except Exception as e:
        print(f"An error occurred: {e}")
        print(traceback.format_exc())
        return Response(f"An internal error occurred: {str(e)}", status=500)


@app.route('/backup', methods=['GET', 'POST'])
def backup():
    try:
        # Fetch parameters based on the request method
        if request.method == 'GET':
            remove_archives = request.args.get('remove_archives', 'no')
            duration = request.args.get('duration')
            bucket = request.args.get('bucket', os.getenv('BUCKET'))
            usernames = request.args.get('usernames')
        elif request.method == 'POST':
            remove_archives = request.form.get('remove_archives', 'no')
            duration = request.form.get('duration')
            bucket = request.form.get('bucket', os.getenv('BUCKET'))
            usernames = request.form.get('usernames')

        # Ensure the required usernames parameter is provided
        if not usernames:
            return Response("The 'usernames' parameter is required", status=400)

        # Build the backup command with appropriate arguments
        command = ['python3', 'lib/backup.py', '--remove_archives',
                   remove_archives, '--usernames', usernames]
        if remove_archives == 'yes' and duration:
            command += ['--duration', duration]
        if bucket:
            command += ['--bucket_name', bucket]

        print(f"Running command: {' '.join(command)}")  # Debug print

        def generate():
            process = subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=os.environ.copy())

            while True:
                output = process.stdout.readline()
                if output:
                    yield f"data:{output}\n\n"
                err = process.stderr.readline()
                if err:
                    yield f"data:{err}\n\n"
                if output == '' and process.poll() is not None:
                    break

        return Response(stream_with_context(generate()), mimetype='text/event-stream')

    except Exception as e:
        return Response(f"An error occurred: {str(e)}", status=500)


@app.route('/pg_function', methods=['GET', 'POST'])
def pg_function():
    try:
        # Fetch parameters based on the request method
        if request.method == 'GET':
            # Default service if not provided
            service_name = request.args.get('service', 'default_service')
            function_name = request.args.get('function')
            # Default schema if not provided
            schema = request.args.get('schema', 'public')
        elif request.method == 'POST':
            # Default service if not provided
            service_name = request.form.get('service', 'default_service')
            function_name = request.form.get('function')
            # Default schema if not provided
            schema = request.form.get('schema', 'public')

        # Ensure the required function_name parameter is provided
        if not function_name:
            return Response("Function parameter is required", status=400)

        # Call the external script and pass the service name, function name, and schema
        command = ['python3', 'lib/pg_function.py',
                   service_name, function_name, schema]

        print(f"Running command: {' '.join(command)}")  # Debug print

        def generate():
            process = subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

            while True:
                output = process.stdout.readline()
                if output:
                    yield f"data:{output}\n\n"
                err = process.stderr.readline()
                if err:
                    yield f"data:{err}\n\n"
                if output == '' and process.poll() is not None:
                    break

        return Response(stream_with_context(generate()), mimetype='text/event-stream')

    except Exception as e:
        print(f"An error occurred: {e}")
        print(traceback.format_exc())
        return Response(f"An internal error occurred: {str(e)}", status=500)


@app.route('/pg_service', methods=['GET', 'POST'])
def pg_service():
    try:
        # Path to the `get_services.py` script
        script_path = "lib/get_services.py"  # Adjust this path as needed

        print(f"Running script: {script_path}")  # Debug print

        def generate():
            process = subprocess.Popen(
                ['python3', script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

            while True:
                output = process.stdout.readline()
                if output:
                    yield f"data:{output}\n\n"
                err = process.stderr.readline()
                if err:
                    yield f"data:{err}\n\n"
                if output == '' and process.poll() is not None:
                    break

        return Response(stream_with_context(generate()), mimetype='text/event-stream')

    except Exception as e:
        print(f"An error occurred: {e}")
        print(traceback.format_exc())
        return Response(f"An internal error occurred: {str(e)}", status=500)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
