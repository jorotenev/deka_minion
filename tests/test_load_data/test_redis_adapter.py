import json
from unittest import TestCase
from uuid import uuid4

from load_data.datastore_adapter import load_to_datastore, Facade
from load_data.datastore_adapter.redis import r, cities_boundaries_template_key, cities_places_template_key, \
    cities_coordinates_template_key, KeyConverter
from load_data.main import parse_raw_input
from tests.test_load_data.test_data import dummy_data
from . import test_redis_db


def drop_db(redis_client):
    [redis_client.delete(key) for key in redis_client.keys("*")]


def ensure_correct_redis_used():
    # we want to verify that we are using a testing db namespace, to avoid polluting the development one
    # we determine that we are connected to the correct namespace by setting a dummy value,
    # then calling "info" on the redis client.
    # this works because the Config is altered in the __init__.py file :)
    dummy = str(uuid4())
    r.set(dummy, 1)
    assert 'db%i' % test_redis_db in r.info("keyspace"), \
        "The redis client is not using the testing db namespace. Testing could corrupt development data"

    # clean-up
    drop_db(r)

    assert len(r.keys("*")) == 0, "There are %i keys" % len(r.keys("*"))
    # good to go


class TestRedisAdapter(TestCase):
    @classmethod
    def setUpClass(cls):
        ensure_correct_redis_used()
        cls.places, cls.metadata = parse_raw_input(dummy_data)

    def tearDown(self):
        drop_db(r)

    def test_correct_top_level_keys(self):
        # dummy_data was loaded from a file with the same format as original data.
        load_to_datastore(self.places, self.metadata)
        TestRedisAdapter.assert_correct_top_level_keys_loaded_in_redis(tester=self, metadata=self.metadata)

    @staticmethod
    def assert_correct_top_level_keys_loaded_in_redis(tester, metadata):
        """
        :param tester: `self` from a method in class extending TestCase
        :param metadata: see the load_data readme
        """
        area_name = metadata.area_name
        expected_keys = [
            "%s%s" % (cities_boundaries_template_key, area_name),
            "%s%s" % (cities_places_template_key, area_name),
            "%s%s" % (cities_coordinates_template_key, area_name)
        ]
        tester.assertEqual(set(expected_keys), set(r.keys("*")))

    def test_correct_data_under_boundaries(self):
        """
        The `cities_boundaries_key` holds a hash.
        The value is the name of a geographical area. the value is a json object with shape like
        {
            "northwest":{"lat":1, "lng":2},
            "southeast":{"lat":1, "lng":2},
        }
        """
        load_to_datastore(self.places, self.metadata)
        TestRedisAdapter.assert_correct_data_under_boundaries(tester=self, metadata=self.metadata)

    @staticmethod
    def assert_correct_data_under_boundaries(tester, metadata):
        area_name = metadata.area_name

        expected_cities_boundaries_keys = [area_name]
        raw_keys = r.keys("*%s*" % cities_boundaries_template_key)

        # keys == ["cities:boundaries:london"]
        keys = [KeyConverter.get_area_name(k) for k in raw_keys]
        tester.assertEqual(set(keys), set(expected_cities_boundaries_keys))

        expected_cities_boundaries_value = json.dumps(
            {
                "northwest": {"lat": metadata.bounding_rectangle["northwest"]["lat"],
                              "lng": metadata.bounding_rectangle["northwest"]["lng"]},
                "southeast": {"lat": metadata.bounding_rectangle["southeast"]["lat"],
                              "lng": metadata.bounding_rectangle["southeast"]["lng"]},
            }
        )
        tester.assertEqual(expected_cities_boundaries_value, r.get(cities_boundaries_template_key + area_name))

    def test_correct_data_under_places(self):
        """
        under the `cities_cities_places_template_key<city_name>` is a hash.
        The key is a place_id as per the Google Places Search API.
        The value is an object, as returned by the same API.
        """
        load_to_datastore(self.places, self.metadata)
        TestRedisAdapter.assert_correct_data_under_places(tester=self, places=self.places, metadata=self.metadata)

    @staticmethod
    def assert_correct_data_under_places(tester, places, metadata):

        area_name = metadata.area_name
        places_key_for_area = cities_places_template_key + area_name

        all_stored_places_dict = r.hgetall(places_key_for_area)

        expected_keys = set(places.keys())

        tester.assertEqual(expected_keys, set(all_stored_places_dict.keys()))

        # now, ensure the objects under the keys are correct
        for place_key, stored_place in all_stored_places_dict.items():
            stored_place = Facade.get_place_data(area_name=area_name, place_key=place_key)
            tester.assertEqual(places[place_key], stored_place)

    def test_correct_data_under_coordinates(self):
        """
        https://redis.io/commands/geoadd
        The `cities_coordinates_template_key<area_name>` holds a sorted set.
        Here we test that the correct items were added to this set.
        An item consists of [lat lng place_id], where place_id is also a key under the
        `cities_places_template_key<area_name>`
        """
        load_to_datastore(self.places, self.metadata)
        TestRedisAdapter.assert_correct_data_under_coordinates(tester=self, metadata=self.metadata, places=self.places)

    @staticmethod
    def assert_correct_data_under_coordinates(tester, places, metadata):
        area_name = metadata.area_name

        # for each place we've loaded query the datastore using the lat+lng of the given place.
        # the query is within a given small radius (for a lack of a better way to search)
        # we expected that the result will be a single item, representing the given place
        for place_id, place in places.items():
            # use the coordinates of a place to query the datastore within a radius
            items = r.georadius(cities_coordinates_template_key + area_name,
                                latitude=place['geometry']['location']['lat'],
                                longitude=place['geometry']['location']['lng'],
                                radius=1,
                                unit='m')
            tester.assertEqual(1, len(items), "Expect that the datastore returned a single"
                                              "items when querying for a location")
            tester.assertEqual(place['place_id'], items[0])


class TestUpdatingExistingArea(TestCase):
    """
    Make sure that updating existing area works (e.g. we've already loaded data for sofia, and we want to update it)
    The test below is actually just loading some data, running all tests from TestRedisAdapter (sanity checking), then loading
    another dataset and running the same sets again.
    """

    @classmethod
    def setUpClass(cls):
        ensure_correct_redis_used()
        cls.places, cls.metadata = parse_raw_input(dummy_data)

    def tearDown(self):
        drop_db(r)

    @staticmethod
    def run_all_tests(tester, places, metadata):
        # test all data in redis
        TestRedisAdapter.assert_correct_data_under_coordinates(tester=tester, places=places, metadata=metadata)
        TestRedisAdapter.assert_correct_data_under_boundaries(tester=tester, metadata=metadata)
        TestRedisAdapter.assert_correct_data_under_places(tester=tester, places=places, metadata=metadata)

    def test_update(self):
        """
        We'd first load half of the dummy data for the given city.
        We'd then load the whole data set.
        We expect that the old data is fully replaced with the new one
        """
        metadata = self.metadata  # the metadata'd stay the same
        places_initial_batch = dict(list(self.places.items())[:len(self.places) // 2])  # get the first half of a dict
        load_to_datastore(places=places_initial_batch, metadata=metadata)

        # sanity checking
        TestUpdatingExistingArea.run_all_tests(tester=self, places=places_initial_batch, metadata=metadata)

        # now load a second batch (which is just all of the available data)
        load_to_datastore(places=self.places, metadata=metadata)
        # and test again
        TestUpdatingExistingArea.run_all_tests(tester=self, places=self.places, metadata=metadata)
