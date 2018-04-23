from redis import StrictRedis

from load_data.config import Config

# instance of the redis client
r = StrictRedis(host=Config.REDIS_HOST, port=Config.REDIST_PORT, db=Config.REDIS_DB)


def load_to_datastore(data):
    pass
