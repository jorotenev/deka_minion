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

        area_name = self.metadata.area_name
        expected_keys = [
            "%s%s" % (cities_boundaries_template_key, area_name),
            "%s%s" % (cities_places_template_key, area_name),
            "%s%s" % (cities_coordinates_template_key, area_name)
        ]
        self.assertEqual(set(expected_keys), set(r.keys("*")))

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
        metadata = self.metadata
        area_name = metadata.area_name

        expected_cities_boundaries_keys = [area_name]
        raw_keys = r.keys("*%s*" % cities_boundaries_template_key)
        # keys == ["cities:boundaries:london"]
        keys = [KeyConverter.get_area_name(k) for k in raw_keys]
        self.assertEqual(set(keys), set(expected_cities_boundaries_keys))

        expected_cities_boundaries_value = json.dumps({
            "northwest": {"lat": metadata.bounding_rectangle["northwest"]["lat"],
                          "lng": metadata.bounding_rectangle["northwest"]["lng"]},
            "southeast": {"lat": metadata.bounding_rectangle["southeast"]["lat"],
                          "lng": metadata.bounding_rectangle["southeast"]["lng"]},
        }
        )
        self.assertEqual(expected_cities_boundaries_value, r.get(cities_boundaries_template_key + area_name))

    def test_correct_data_under_places(self):
        """
        under the `cities_cities_places_template_key<city_name>` is a hash.
        The key is a place_id as per the Google Places Search API.
        The value is an object, as returned by the same API.
        """
        load_to_datastore(self.places, self.metadata)

        area_name = self.metadata.area_name
        places_key_for_area = cities_places_template_key + area_name

        all_stored_places_dict = r.hgetall(places_key_for_area)

        expected_keys = set(dummy_data['places'].keys())

        self.assertEqual(expected_keys, set(all_stored_places_dict.keys()))

        # ensure the objects under the keys are correct
        for place_key, stored_place in all_stored_places_dict.items():
            stored_place = Facade.get_place_data(area_name=area_name, place_key=place_key)
            self.assertEqual(self.places[place_key], stored_place)

    def test_correct_data_under_coordinates(self):
        """
        https://redis.io/commands/geoadd
        The `cities_coordinates_template_key<area_name>` holds a sorted set.
        Here we test that the correct items were added to this set.
        An item consists of [lat lng place_id], where place_id is also a key under the
        `cities_places_template_key<area_name>`
        """
        load_to_datastore(self.places, self.metadata)
        area_name = self.metadata.area_name

        # for each place we've loaded query the datastore using the lat+lng of the given place.
        # the query is within a given small radius (for a lack of a better way to search)
        # we expected that the result will be a single item, representing the given place
        for place_id, place in self.places.items():
            # use the coordinates of a place to query the datastore within a radius
            items = r.georadius(cities_coordinates_template_key + area_name,
                                latitude=place['geometry']['location']['lat'],
                                longitude=place['geometry']['location']['lng'],
                                radius=1,
                                unit='m')
            self.assertEqual(1, len(items), "Expect that the datastore returned a single"
                                            "items when querying for a location")
            self.assertEqual(place['place_id'], items[0])
