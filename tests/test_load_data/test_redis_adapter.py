import json
from unittest import TestCase
from uuid import uuid4

from load_data.datastore_adapter import load_to_datastore, RedisFacade
from load_data.datastore_adapter.redis import r, cities_boundaries_template_key, cities_places_template_key, \
    cities_coordinates_template_key
from load_data.main import parse_raw_input
from tests.test_load_data.test_data import dummy_data_sofia, dummy_data_leuven
from . import test_redis_db


class TestRedisMixin(TestCase):
    @classmethod
    def setUpClass(cls):
        ensure_correct_redis_db_during_testing()
        # read data
        cls.places_sofia, cls.metadata_sofia = parse_raw_input(dummy_data_sofia)

    def tearDown(self):
        drop_db(r)


class TestRedisAdapter(TestRedisMixin):

    def test_correct_top_level_keys(self):
        # dummy_data was loaded from a file with the same format as original data.
        load_to_datastore(self.places_sofia, self.metadata_sofia)
        CommonAssertions.check_exclusive_correct_top_level_keys_loaded_in_redis(tester=self,
                                                                                expected_areas=[
                                                                                    self.metadata_sofia.area_name])

    def test_correct_data_under_boundaries(self):
        """
        The `cities_boundaries_key` holds a hash.
        The value is the name of a geographical area. the value is a json object with shape like
        {
            "northwest":{"lat":1, "lng":2},
            "southeast":{"lat":1, "lng":2},
        }
        """
        load_to_datastore(self.places_sofia, self.metadata_sofia)

        area_name = self.metadata_sofia.area_name

        # ensure only the boundary for the correct single area is loaded
        redis_boundaries_keys = r.keys("*%s*" % cities_boundaries_template_key)  # returns a list
        self.assertEqual(set([cities_boundaries_template_key + area_name]), set(redis_boundaries_keys))

        CommonAssertions.check_correct_boundaries_for_area(tester=self, metadata=self.metadata_sofia)

    def test_correct_data_under_places(self):
        """
        under the `cities_cities_places_template_key<city_name>` is a hash.
        The key is a place_id as per the Google Places Search API.
        The value is an object, as returned by the same API.
        """
        load_to_datastore(self.places_sofia, self.metadata_sofia)
        CommonAssertions.check_correct_data_under_places(tester=self, places=self.places_sofia,
                                                         metadata=self.metadata_sofia)

    def test_correct_data_under_coordinates(self):
        """
        https://redis.io/commands/geoadd
        The `cities_coordinates_template_key<area_name>` holds a sorted set.
        Here we test that the correct items were added to this set.
        An item consists of [lat lng place_id], where place_id is also a key under the
        `cities_places_template_key<area_name>`
        """
        load_to_datastore(self.places_sofia, self.metadata_sofia)
        CommonAssertions.check_correct_data_under_coordinates(tester=self, metadata=self.metadata_sofia,
                                                              places=self.places_sofia)


class TestUpdatingExistingArea(TestRedisMixin):
    """
    Make sure that updating existing area works (e.g. we've already loaded data for sofia, and we want to update it)
    The test below is actually just loading some data, running all tests from TestRedisAdapter (sanity checking), then loading
    another dataset and running the same sets again.
    """

    def test_update(self):
        """
        We'd first load half of the dummy data for the given city.
        We'd then load the whole data set.
        We expect that the old data is fully replaced with the new one
        """
        metadata = self.metadata_sofia  # the metadata'd stay the same
        places_initial_batch = dict(
            list(self.places_sofia.items())[:len(self.places_sofia) // 2])  # get the first half of a dict
        load_to_datastore(places=places_initial_batch, metadata=metadata)

        # sanity checking
        CommonAssertions.run_all_tests_single_area_loaded(tester=self, places=places_initial_batch, metadata=metadata)

        # now load a second batch (which is just all of the available data)
        load_to_datastore(places=self.places_sofia, metadata=metadata)
        # and test again
        CommonAssertions.run_all_tests_single_area_loaded(tester=self, places=self.places_sofia, metadata=metadata)


class TestMultipleGeoAreas(TestRedisMixin, ):
    @classmethod
    def setUpClass(cls):
        super(TestMultipleGeoAreas, cls).setUpClass()
        cls.places_leuven, cls.metadata_leuven = parse_raw_input(dummy_data_leuven)

    def test_load_two_areas(self):
        data = [
            [self.metadata_sofia, self.places_sofia],
            [self.metadata_leuven, self.places_leuven]
        ]

        for metadata, places in data:
            print("loading data for %s" % metadata.area_name)
            load_to_datastore(places=places, metadata=metadata)

        for metadata, places in data:
            print("run all tests for %s" % metadata.area_name)
            CommonAssertions.run_all_tests_single_area_loaded(tester=self, places=places, metadata=metadata)
            CommonAssertions.check_exclusive_correct_top_level_keys_loaded_in_redis(tester=self, expected_areas=[
                self.metadata_leuven.area_name,
                self.metadata_sofia.area_name,
            ])


class CommonAssertions:
    """
    the methods don't make the assumption that there's a single area loaded - e..g. they work even if more than
    one area are loaded in redis
    """

    @staticmethod
    def run_all_tests_single_area_loaded(tester, places, metadata):
        # test all data in redis for a single area.

        CommonAssertions.check_correct_data_under_coordinates(tester=tester, places=places, metadata=metadata)
        CommonAssertions.check_correct_boundaries_for_area(tester=tester, metadata=metadata)
        CommonAssertions.check_correct_data_under_places(tester=tester, places=places, metadata=metadata)

    @staticmethod
    def check_exclusive_correct_top_level_keys_loaded_in_redis(tester: TestCase, expected_areas):
        """
        only the keys for the selected areas are allowed. e.g. if for a given area there're extra keys, this
        check will bark
        """
        expected_keys = []
        for area_name in expected_areas:
            # we expect each of the keys below for each area
            expected_keys.append("%s%s" % (cities_boundaries_template_key, area_name))
            expected_keys.append("%s%s" % (cities_places_template_key, area_name))
            expected_keys.append("%s%s" % (cities_coordinates_template_key, area_name))

        tester.assertEqual(set(expected_keys), set(r.keys("*")))

    @staticmethod
    def check_correct_boundaries_for_area(tester, metadata):
        area_name = metadata.area_name
        expected_boundary_rectangle = json.dumps(
            {
                "northwest": {"lat": metadata.bounding_rectangle["northwest"]["lat"],
                              "lng": metadata.bounding_rectangle["northwest"]["lng"]},
                "southeast": {"lat": metadata.bounding_rectangle["southeast"]["lat"],
                              "lng": metadata.bounding_rectangle["southeast"]["lng"]},
            }
        )

        loaded_value = r.get(cities_boundaries_template_key + area_name)

        tester.assertEqual(expected_boundary_rectangle, loaded_value)

    @staticmethod
    def check_correct_data_under_places(tester, places, metadata):

        area_name = metadata.area_name
        places_key_for_area = cities_places_template_key + area_name

        all_stored_places_dict = r.hgetall(places_key_for_area)

        expected_keys = set(places.keys())

        tester.assertEqual(expected_keys, set(all_stored_places_dict.keys()))

        # now, ensure the objects under the keys are correct
        for place_key, stored_place in all_stored_places_dict.items():
            stored_place = RedisFacade.get_place_data(area_name=area_name, place_key=place_key)
            tester.assertEqual(places[place_key], stored_place)

    @staticmethod
    def check_correct_data_under_coordinates(tester, places, metadata):
        area_name = metadata.area_name

        # for each place we've loaded query the datastore using the lat+lng of the given place.
        # the query is within a given small radius (for a lack of a better way to search)
        # we expected that the result will be one (or at least just a very few) place
        for place_id, place in places.items():
            # use the coordinates of a place to query the datastore within a radius
            db_items_ids = r.georadius(cities_coordinates_template_key + area_name,
                                       latitude=place['geometry']['location']['lat'],
                                       longitude=place['geometry']['location']['lng'],
                                       radius=1,
                                       unit='m')
            tester.assertTrue(len(db_items_ids) >= 1,
                              "Expecteed at least one item when querying a location for which a place have been added")

            tester.assertIn(place['place_id'], db_items_ids)


def drop_db(redis_client):
    [redis_client.delete(key) for key in redis_client.keys("*")]


def ensure_correct_redis_db_during_testing():
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
