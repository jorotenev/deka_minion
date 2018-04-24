from unittest import TestCase
from uuid import uuid4

from load_data.datastore_adapter import load_to_datastore
# import only after the config was changed
from load_data.datastore_adapter.redis import r
from load_data.main import parse_raw_input
from tests.test_load_data.test_data import dummy_data
from . import test_redis_db


def drop_db(redis_client):
    [redis_client.delete(key) for key in redis_client.keys("*")]


class TestRedisAdapter(TestCase):
    @classmethod
    def setUpClass(cls):
        # we want to verify that we are using a testing db namespace, to avoid polluting the development env
        # we determine that we are connected to the correct namespace by setting a dummy value,
        # then calling "info" on the redis client.
        dummy = str(uuid4())
        r.set(dummy, 1)
        assert 'db%i' % test_redis_db in r.info("keyspace"), \
            "The redis client is not using the testing db namespace. Testing could corrupt development data"

        # clean-up
        drop_db(r)

        assert len(r.keys("*")) == 0, "There are %i keys" % len(r.keys("*"))
        # good to go

    def tearDown(self):
        drop_db(r)

    def test_correct_top_level_keys(self):
        # dummy_data was loaded from a file with the same format as original data.
        places, metadata = parse_raw_input(dummy_data)
        load_to_datastore(places, metadata)

        area_name = metadata.area_name
        expected_keys = [
            "cities:boundaries",
            "cities:places:%s" % area_name,
            "cities:coordinates:%s" % area_name
        ]
        self.assertEqual(set(expected_keys), set(r.keys("*")))
