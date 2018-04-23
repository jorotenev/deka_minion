from unittest import TestCase
from uuid import uuid4

from load_data.config import Config

test_redis_db = 9
Config.REDIS_DB = test_redis_db

dummy_places = {}
for i in range(10):
    dummy_places[uuid4()] = {
        "geometry": {
            "lat": i + 1,
            "lng": i + 2
        }
    }


def drop_db(redis_client):
    [redis_client.delete(key) for key in redis_client.keys("*")]


class TestRedisAdapter(TestCase):
    @classmethod
    def setUpClass(cls):
        from load_data.redis_adapter.redis import r
        # we want to verify that we are using a testing db namespace, to avoid polluting the development env
        # we determine that we are connected to the correct namespace by setting a dummy value,
        # then calling "info" on the redis client.
        dummy = str(uuid4())
        r.set(dummy, 1)
        assert 'db%i' % test_redis_db in r.info(
            "keyspace"), "The redis client is not using the testing db namespace. Testing could corrupt development data"

        # clean-up
        drop_db(r)

        assert len(r.keys("*")) == 0, "There are %i keys" % len(r.keys("*"))
        # good to go

    def tearDown(self):
        drop_db(self.r)

    def teslt_simple(self):
        pass
        # use the redis_adapter to load the data
        # use the raw_client to verify that the corect items were loaded under the correct key
