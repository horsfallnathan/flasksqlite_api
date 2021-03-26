import json
import pytest
import sqlite3
import time
import unittest

from app import app


class SensorRoutesTestCases(unittest.TestCase):

    def setUp(self):
        # Setup the SQLite DB
        conn = sqlite3.connect('test_database.db')
        conn.execute('DROP TABLE IF EXISTS readings')
        conn.execute(
            'CREATE TABLE IF NOT EXISTS readings (device_uuid TEXT, type TEXT, value INTEGER, date_created INTEGER)')

        self.device_uuid = 'test_device'

        # Setup some sensor data
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Tests would usually take variable amounts of time to run, so saving a time anchor will come
        # handy while testing endpoints requiring time ranges

        # Create time anchors
        time_anchor = int(time.time()) - 100
        time_anchor_1 = int(time.time()) - 50
        time_anchor_2 = int(time.time())

        # Add time anchors to test class
        self.time_anchor = time_anchor
        self.time_anchor_1 = time_anchor_1
        self.time_anchor_2 = time_anchor_2

        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 22, time_anchor))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 50, time_anchor_1))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'humidity', 50, time_anchor_1))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 100, time_anchor_2))

        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    ('other_uuid', 'temperature', 22, time_anchor_2))
        conn.commit()

        app.config['TESTING'] = True

        self.client = app.test_client

        # Define test-wide cursor and connection
        self.cur = cur
        self.conn = conn

    def test_device_readings_get(self):
        # Given a device UUID
        # When we make a request with the given UUID
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid))

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        # And the response data should have four sensor readings
        self.assertTrue(len(json.loads(request.data)) == 4)

    def test_device_readings_post(self):
        # Given a device UUID
        # When we make a request with the given UUID to create a reading
        request = self.client().post('/devices/{}/readings/'.format(self.device_uuid), data=
        json.dumps({
            'type': 'temperature',
            'value': 100
        }))

        # Then we should receive a 201
        self.assertEqual(request.status_code, 201)

        # And when we check for readings in the db
        self.cur.execute('select * from readings where device_uuid="{}"'.format(self.device_uuid))
        rows = self.cur.fetchall()

        # We should have five
        self.assertTrue(len(rows) == 5)

    def test_device_readings_post_validation(self):
        # Given a device UUID
        # When we make a request with the given UUID and incorrectly formatted input to create a reading

        # !Todo refactor repetitive code with pytest's fixtures or parametrize methods.

        with self.subTest():
            # Test for value inputs above threshold
            request = self.client().post('/devices/{}/readings/'.format(self.device_uuid), data=json.dumps({
                'type': 'temperature',
                'value': 130
            }))
            # Then we should receive a 400
            self.assertEqual(request.status_code, 400, 'values above threshold')

        with self.subTest():
            # Test for negative value inputs ie. values below the threshold
            request1 = self.client().post('/devices/{}/readings/'.format(self.device_uuid), data=json.dumps({
                'type': 'temperature',
                'value': -20
            }))
            # Then we should receive a 400
            self.assertEqual(request1.status_code, 400, 'Values below threshold')

        with self.subTest():
            # Test for wrong data type - string in this case
            request2 = self.client().post('/devices/{}/readings/'.format(self.device_uuid), data=json.dumps({
                'type': 'temperature',
                'value': 'let me in, pretty please'
            }))
            # Then we should receive a 400
            self.assertEqual(request2.status_code, 400, 'String types')

        with self.subTest():
            # And when we check for readings in the db
            self.cur.execute('select * from readings where device_uuid="{}"'.format(self.device_uuid))
            rows = self.cur.fetchall()

            # We should have four
            self.assertTrue(len(rows) == 4, 'DB remains unchanged')

    def test_device_readings_get_temperature(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's temperature data only.

        - My remarks:
        This test also passes if there is no type in the returned reading data.
        To fix this, we could use `reading.get('type')` in place of `reading['type']`,
        but this should be fine because our validation ensures all readings should have
        a sensor type.
        """
        response = self.client().get('/devices/{}/readings/?type=temperature'.format(self.device_uuid))
        response_list = json.loads(response.data)

        # Checks that all device reading types are equal to temperature. This covers 'humidity' or future sensor types
        has_other_types = any(reading['type'] != 'temperature' for reading in response_list)

        self.assertFalse(has_other_types)

    def test_device_readings_get_humidity(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's humidity data only.
        """
        # Given a type query equal to humidity
        # When we make a request to the readings endpoint with this type
        response = self.client().get('/devices/{}/readings/?type=humidity'.format(self.device_uuid))
        response_list = json.loads(response.data)

        # And check that no device reading has a different sensor type. This covers 'temperature' or future
        # sensor types
        has_other_types = any(reading['type'] != 'humidity' for reading in response_list)

        # Then we should find no reading with a different sensor type
        self.assertFalse(has_other_types)
        # And get a status code of 200
        self.assertEqual(response.status_code, 200)

    def test_device_readings_get_past_dates(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's sensor data over
        a specific date range. We should only get the readings
        that were created in this time range.

        - My Remarks:
        No format was given for time input so I worked with the current epoch state, also under the
        assumption that the endpoint will be called from a device in kiosk mode. The endpoint could
        almost trivially be extended to handle datetime conversions if another format is to be supplied.
        """
        # Given a type query equal to temperature
        with self.subTest():
            # When we make a request to the readings endpoint with a start time
            response = self.client().get('/devices/{}/readings/?type=temperature?start={}'.format(self.device_uuid,
                                                                                                  self.time_anchor_1))
            response_list = json.loads(response.data)

            # and check that date_created for all returned readings are greater or equal to the sent start value
            is_within_range = all(reading['date_created'] >= self.time_anchor_1 for reading in response_list)

            # Then we should have True
            self.assertTrue(is_within_range)

        with self.subTest():
            # When we make a request to the readings endpoint with an end time
            response = self.client().get('/devices/{}/readings/?type=temperature&end={}'.format(self.device_uuid,
                                                                                                self.time_anchor_1))
            response_list = json.loads(response.data)

            # and check that date_created for all returned readings are less or equal to the sent start value
            is_within_range = all(reading['date_created'] <= self.time_anchor_1 for reading in response_list)

            # Then we should have True
            self.assertTrue(is_within_range)

        with self.subTest():
            # When we make a request to the readings endpoint with start and end times
            response = self.client().get('/devices/{}/readings/?type=temperature&start={}&end={}'.format(
                self.device_uuid, self.time_anchor_1, self.time_anchor_2))
            response_list = json.loads(response.data)

            # and check that date_created for all returned readings are between the start and end times, both inclusive
            is_within_range = all(
                self.time_anchor_1 <= reading['date_created'] <= self.time_anchor_2 for reading in response_list)

            # Then we should have True
            self.assertTrue(is_within_range)

    def test_device_readings_min(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's min sensor reading.
        """
        # Given a device with sensor type - 'temperature'
        # When we make a request to the min endpoint with this type
        response = self.client().get('/devices/{}/readings/min/?type=temperature'.format(self.device_uuid))
        response_dict = json.loads(response.data)

        expected_response = {"value": 22}

        # Then we should get a dict having a key-value pair of value, 22
        self.assertEqual(response_dict, expected_response)
        # And get a status code of 200
        self.assertEqual(response.status_code, 200)

    def test_device_readings_max(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's max sensor reading.
        """
        # Given a device with sensor type - 'temperature'
        # When we make a request to the max endpoint with this type
        response = self.client().get('/devices/{}/readings/max/?type=temperature'.format(self.device_uuid))
        response_dict = json.loads(response.data)

        expected_response = {"value": 100}

        # Then we should get a dict having a key-value pair of value, 22
        self.assertEqual(response_dict, expected_response)
        # And get a status code of 200
        self.assertEqual(response.status_code, 200)

    def test_device_readings_median(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's median sensor reading.
        """
        # Given a device with sensor type - 'temperature'
        # When we make a request to the median endpoint with this type
        response = self.client().get('/devices/{}/readings/median/?type=temperature'.format(self.device_uuid))
        response_dict = json.loads(response.data)

        expected_response = {"value": 50}

        # Then we should get a dict having a key-value pair of value, 22
        self.assertEqual(response_dict, expected_response)
        # And get a status code of 200
        self.assertEqual(response.status_code, 200)

    def test_device_readings_mean(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's mean sensor reading value.
        """
        # Given a device with sensor type - 'temperature'
        # When we make a request to the mean endpoint with this type
        response = self.client().get('/devices/{}/readings/mean/?type=temperature'.format(self.device_uuid))
        response_dict = json.loads(response.data)

        expected_response = {"value": 57.33}

        # Then we should get a dict having a key-value pair of value, 22
        self.assertEqual(response_dict, expected_response)
        # And get a status code of 200
        self.assertEqual(response.status_code, 200)

    def test_device_readings_mode(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's mode sensor reading value.
        """
        # Add one more entry to db to create a clear mode
        self.cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                         (self.device_uuid, 'temperature', 22, self.time_anchor - 20))
        self.conn.commit()
        # Given a device with sensor type - 'temperature'
        # When we make a request to the mode endpoint with this type
        response = self.client().get('/devices/{}/readings/mode/?type=temperature'.format(self.device_uuid))
        response_dict = json.loads(response.data)

        expected_response = {"value": 22}

        # Then we should get a dict having a key-value pair of value, 22
        self.assertEqual(response_dict, expected_response)
        # And get a status code of 200
        self.assertEqual(response.status_code, 200)

    def test_device_readings_quartiles(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's 1st and 3rd quartile
        sensor reading value.
        """
        # Add one more entry to db
        self.cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                         (self.device_uuid, 'temperature', 75, self.time_anchor))
        self.conn.commit()
        # Given a device with sensor type - 'temperature'
        # When we make a request to the median endpoint with this type
        response = self.client().get('/devices/{}/readings/quartiles/?type=temperature'.format(self.device_uuid))
        response_dict = json.loads(response.data)

        expected_response = {'quartile_1': 36, 'quartile_3': 87.5}

        # Then we should get a dict having a key-value pair of value, 22
        self.assertEqual(response_dict, expected_response)
        # And get a status code of 200
        self.assertEqual(response.status_code, 200)

    def test_device_readings_summary(self):
        """
        This tests that when a GET request is made to the summary endpoint a breakdown of device information is sent
        back.
        """
        # When we make a request to the summary endpoint
        response = self.client().get('/devices/summary/')
        response_dict = json.loads(response.data)

        other_uuid_summary = [device for device in response_dict if device['device_uuid'] == 'other_uuid'][0]

        expected_response = {
            "device_type": "temperature",
            "device_uuid": "other_uuid",
            "max_reading_value": 22,
            "mean_reading_value": 22.0,
            "median_value": 22.0,
            "number_of_readings": 1,
            "quartile_1_value": 22.0,
            "quartile_3_value": 22.0
          }

        # Then the other_uuid summary returned should equal the expected result
        self.assertEqual(other_uuid_summary, expected_response)

        # The result should be grouped by sensor types, so we should have 3 items
        self.assertEqual(len(response_dict), 3)
        # And get a status code of 200
        self.assertEqual(response.status_code, 200)
