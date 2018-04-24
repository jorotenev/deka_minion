from redis import StrictRedis

from load_data.config import Config
# instance of the redis client
from load_data.deka_types import Metadata

r = StrictRedis(host=Config.REDIS_HOST, port=Config.REDIST_PORT, db=Config.REDIS_DB)


def load_to_datastore(places, metadata: Metadata):
    """

    :param places: dict. the keys are place_id (as per Google Places Search API). The value is a place object, as returned by
    the same API.
    :param metadata:
    :return boolean - True if successfully added
    """
    pass
