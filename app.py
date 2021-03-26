from flask import Flask, render_template, request, Response
from flask.json import jsonify
import json
import sqlite3
import time

# added packages
from cerberus import Validator
from sqlite3 import Error
import numpy as np
from flask import g

app = Flask(__name__)

# Setup the SQLite DB
conn = sqlite3.connect('database.db')
conn.execute('CREATE TABLE IF NOT EXISTS readings (device_uuid TEXT, type TEXT, value INTEGER, date_created INTEGER)')
conn.close()


def get_db():
    """
    Create a DB context if it doesn't already exist and connect to the a database depending on the environment ie.
    production or testing.
    * return -> Database instance
    """
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect('test_database.db' if app.config['TESTING'] else 'database.db')

        # Enable row factory to make responses more malleable
        db.row_factory = sqlite3.Row
        return db

# Terminate db connection when application exits
@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        print(exception)
        db.close()


# Percentile helper class
class Percentile(object):
    """
    Passed as a user function to sqlite and used in calculating median, and 1st, and 3rd quartiles
    """
    def __init__(self):
        self.arr = []
        self.percentile = ""

    def step(self, value, per):
        self.arr.append(value)
        self.percentile = per

    def finalize(self):
        return np.nanpercentile(self.arr, self.percentile, interpolation='midpoint')


# Build sql query string
def metric_query_builder(sensor_type, query_template, start=None, end=None):
    """
    Build sql query string with available conditional parameters
    * sensor_type -> The type of sensor value a client is looking for
    * start -> The epoch start time from which to begin search
    * end -> The epoch end time to stop search
    * return -> (query_template, params)
    """

    # Initialize params list
    params = [sensor_type]
    # conditionally append where clauses to query template
    if start is not None:
        params.append(start)
        query_template += "and date_created >= ?"
    if end is not None:
        params.append(end)
        query_template += "and date_created <= ?"

    # Return query and params tuple
    return query_template, tuple(params)


@app.route('/devices/<string:device_uuid>/readings/', methods=['POST', 'GET'])
def request_device_readings(device_uuid):
    # Get the db and cursor objects
    db = get_db()
    cur = db.cursor()

    if request.method == 'POST':
        # Grab the post parameters
        post_data = json.loads(request.data)

        # Validate post parameters
        write_schema = {"type": {'type': 'string'}, 'value': {'type': 'integer', 'min': 0, 'max': 100}}

        v = Validator(write_schema)
        is_valid = v.validate(post_data)

        # return error if validation fails
        if not is_valid:
            return "invalid input", 400

        # Extract post parameters
        sensor_type = post_data.get('type')
        value = post_data.get('value')
        date_created = post_data.get('date_created', int(time.time()))

        # Insert data into db
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (device_uuid, sensor_type, value, date_created))

        db.commit()

        # Return success
        return 'success', 201
    else:
        # Build sql query string
        def query_builder(sensor_type, query_template, start=None, end=None):
            """
            Build sql query string with available conditional parameters
            * sensor_type -> The type of sensor value a client is looking for
            * start -> The epoch start time from which to begin search
            * end -> The epoch end time to stop search
            * return -> (query_template, params)
            """

            # Initialize params list
            params = []
            # conditionally append where clauses to query template
            if sensor_type is not None:
                params.append(sensor_type)
                query_template += "and type = ?"

            if start is not None:
                params.append(start)
                query_template += "and date_created >= ?"
            if end is not None:
                params.append(end)
                query_template += "and date_created <= ?"

            # Return query and params tuple
            return query_template, tuple(params)

        # Query template
        query_template = "select * from readings where device_uuid=?"

        # Extract query parameters
        sensor_type = request.args.get('type') or None
        epoch_start = request.args.get('start') or None
        epoch_end = request.args.get('end') or None

        # Build query string
        get_query, params = query_builder(sensor_type, query_template, epoch_start, epoch_end)
        params_tuple = (device_uuid,) + params

        # Execute the query
        cur.execute(get_query, params_tuple)
        rows = cur.fetchall()

        # Return the JSON
        return jsonify([dict(zip(['device_uuid', 'type', 'value', 'date_created'], row)) for row in rows]), 200


@app.route('/devices/<string:device_uuid>/readings/max/', methods=['GET'])
def request_device_readings_max(device_uuid):
    """
    This endpoint allows clients to GET the max sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """
    # Get the db and cursor objects
    db = get_db()
    cur = db.cursor()

    # Extract query parameters
    sensor_type = request.args.get('type')

    # Check for sensor type
    if sensor_type is None:
        return "sensor type is required", 400

    # Extract optional query parameters
    epoch_start = request.args.get('start')
    epoch_end = request.args.get('end')

    # Make query template
    max_query_template = 'select max(value) as value from readings where device_uuid=? and type=? '

    # Build sql query string with parameters
    max_query, params = metric_query_builder(sensor_type, max_query_template, epoch_start, epoch_end)
    params_tuple = (device_uuid,) + params

    try:
        # Execute the query
        cur.execute(max_query, params_tuple)
        row = cur.fetchone()

        if row is None:
            return {"value": "Null"}, 200
        else:
            # Return the JSON
            return dict(zip(['value'], row)), 200

    except Error as err:
        print(err)
        # Return error message
        return "Error getting max value", 500


@app.route('/devices/<string:device_uuid>/readings/min/', methods=['GET'])
def request_device_readings_min(device_uuid):
    """
    This endpoint allows clients to GET the min sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """
    # Get the db and cursor objects
    db = get_db()
    cur = db.cursor()

    # Extract query parameters
    sensor_type = request.args.get('type')

    # Check for sensor type
    if sensor_type is None:
        return "sensor type is required", 400

    # Extract optional query parameters
    epoch_start = request.args.get('start')
    epoch_end = request.args.get('end')

    # Make query template
    min_query_template = 'select min(value) as value from readings where device_uuid=? and type=? '

    # Build sql query string with parameters
    min_query, params = metric_query_builder(sensor_type, min_query_template, epoch_start, epoch_end)
    params_tuple = (device_uuid,) + params

    try:
        # Execute the query
        cur.execute(min_query, params_tuple)
        row = cur.fetchone()

        # handle empty responses
        if row is None:
            return {"value": "null"}, 200
        else:
            # Return the JSON
            return dict(zip(['value'], row)), 200

    except Error as err:
        print(err)
        # Return error message
        return "Error getting min value", 500


@app.route('/devices/<string:device_uuid>/readings/median/', methods=['GET'])
def request_device_readings_median(device_uuid):
    """
    This endpoint allows clients to GET the median sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """
    # Get the db and cursor objects
    db = get_db()
    cur = db.cursor()

    # Register percentile helper function
    db.create_aggregate("percentile", 2, Percentile)

    # Extract mandatory query parameter
    sensor_type = request.args.get('type')

    # Check for sensor type
    if sensor_type is None:
        return "sensor type is required", 400

    # Extract optional query parameters
    epoch_start = request.args.get('start')
    epoch_end = request.args.get('end')

    # Make query template
    median_query_template = 'select percentile(value, 50) as value from readings where device_uuid=? and type=? '

    # Build sql query string with parameters
    median_query, params = metric_query_builder(sensor_type, median_query_template, epoch_start, epoch_end)

    params_tuple = (device_uuid,) + params

    try:
        # Execute the query
        cur.execute(median_query, params_tuple)
        row = cur.fetchone()

        # handle empty responses
        if row is None:
            return {"value": "null"}, 200
        else:
            # Return the JSON
            return dict(zip(['value'], row)), 200

    except Error as err:
        print(err)
        # Return error message
        return "Error getting max value", 500


@app.route('/devices/<string:device_uuid>/readings/mean/', methods=['GET'])
def request_device_readings_mean(device_uuid):
    """
    This endpoint allows clients to GET the mean sensor readings for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """
    # Get the db and cursor objects
    db = get_db()
    cur = db.cursor()

    # Extract query parameters
    sensor_type = request.args.get('type')

    # Check for sensor type
    if sensor_type is None:
        return "sensor type is required", 400

    # Extract optional query parameters
    epoch_start = request.args.get('start')
    epoch_end = request.args.get('end')

    # Make query template
    mean_query_template = 'select round(avg(value),2) as value from readings where device_uuid=? and type=? '

    # Build sql query string with parameters
    mean_query, params = metric_query_builder(sensor_type, mean_query_template, epoch_start, epoch_end)

    params_tuple = (device_uuid,) + params

    try:
        # Execute the query
        cur.execute(mean_query, params_tuple)
        row = cur.fetchone()

        # handle empty responses
        if row is None:
            return {"value": "null"}, 200
        else:
            # Return the JSON
            return dict(zip(['value'], row)), 200

    except Error as err:
        print(err)
        # Return error message
        return "Error getting mean value", 500


@app.route('/devices/<string:device_uuid>/readings/mode/', methods=['GET'])
def request_device_readings_mode(device_uuid):
    """
    This endpoint allows clients to GET the mode sensor readings for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """
    # Get the db and cursor objects
    db = get_db()
    cur = db.cursor()

    # Extract query parameters
    sensor_type = request.args.get('type')

    # Check for sensor type
    if sensor_type is None:
        return "sensor type is required", 400

    # Extract optional query parameters
    epoch_start = request.args.get('start')
    epoch_end = request.args.get('end')

    # Make query template
    mode_query_template = 'select value as value from readings where device_uuid=? and type=? '

    # Build sql query string with parameters
    mode_query, params = metric_query_builder(sensor_type, mode_query_template, epoch_start, epoch_end)

    params_tuple = (device_uuid,) + params

    print(mode_query + 'group by value, order by count(*) desc limit 1', params_tuple)
    try:
        # Execute the query
        cur.execute(mode_query + 'group by value order by count(*) desc', params_tuple)
        row = cur.fetchone()

        # handle empty responses
        if row is None:
            return {"value": "null"}, 200
        else:
            # Return the JSON
            return dict(zip(['value'], row)), 200

    except Error as err:
        print(err)
        # Return error message
        return "Error getting mode value", 500


@app.route('/devices/<string:device_uuid>/readings/quartiles/', methods=['GET'])
def request_device_readings_quartiles(device_uuid):
    """
    This endpoint allows clients to GET the 1st and 3rd quartiles for sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """
    # Get the db and cursor objects
    db = get_db()
    cur = db.cursor()

    # Register percentile helper function
    db.create_aggregate("percentile", 2, Percentile)

    # Extract query parameters
    sensor_type = request.args.get('type')

    # Check for sensor type
    if sensor_type is None:
        return "sensor type is required", 400

    # Extract optional query parameters
    epoch_start = request.args.get('start')
    epoch_end = request.args.get('end')

    # Make query template
    quartile_query_template = 'select percentile(value, 25) as quartile_1, percentile(value, 75) as quartile_3 from ' \
                              'readings where device_uuid=? and type=? '

    # Build sql query string with parameters
    quartile_query, params = metric_query_builder(sensor_type, quartile_query_template, epoch_start, epoch_end)

    params_tuple = (device_uuid,) + params
    try:
        # Execute the query
        cur.execute(quartile_query, params_tuple)
        row = cur.fetchone()

        # Return the JSON
        return dict(zip(['quartile_1', 'quartile_3'], row)), 200

    except Error as err:
        print(err)
        # Return error message
        return "Error getting max value", 500


@app.route('/devices/summary/', methods=['GET'])
def request_device_summary():
    # Get the db and cursor objects
    db = get_db()
    cur = db.cursor()

    # Register percentile helper function
    db.create_aggregate("percentile", 2, Percentile)

    # Query template
    query_template = "select device_uuid, count(*) as number_of_readings, max(value) as max_reading_value, " \
                     "round(avg(value),2) as mean_reading_value, percentile(value, 25) as  quartile_1_value," \
                     "percentile(value, 75) as quartile_3_value, percentile(value, 50) as median_value, " \
                     "type as device_type from readings group by device_uuid, type order by count(device_uuid) desc"

    try:
        # Execute the query
        cur.execute(query_template)
        rows = cur.fetchall()

        # Return the JSON
        return jsonify([dict(zip(['device_uuid', 'number_of_readings', 'max_reading_value', 'mean_reading_value',
                                  'quartile_1_value', 'quartile_3_value', 'median_value', 'device_type'], row)) for row
                        in rows]), 200
    except Error:
        # Return error message
        return "Error getting max value", 500


if __name__ == '__main__':
    app.run()
