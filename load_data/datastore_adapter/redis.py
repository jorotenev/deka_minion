import json

from redis import StrictRedis

from load_data.config import Config
# instance of the redis client
from load_data.deka_types import Metadata, GeoRectangle

r = StrictRedis(host=Config.REDIS_HOST, port=Config.REDIST_PORT, db=Config.REDIS_DB)
cities_boundaries_key = "cities:boundaries"
# e.g. cities:places:london
cities_places_template_key = "cities:places:"
# e.g. cities:coordinates:london
cities_coordinates_template_key = "cities:coordinates:"


def load_to_datastore(places, metadata: Metadata):
    """

    :param places: dict. the keys are place_id (as per Google Places Search API). The value is a place object, as returned by
    the same API.
    :param metadata:
    :return boolean - True if successfully added
    """
    pass


def deserialize(serialized):
    return json.loads(serialized)


def serialize(raw):
    return json.dumps(raw)


class QueryFacade:
    def __init__(self):
        pass

    @staticmethod
    def get_boundaries_for_area(area_name) -> GeoRectangle:
        raise NotImplemented()

    @staticmethod
    def get_place_data(area_name, place_key):
        raise NotImplemented()

    @staticmethod
    def get_all_places_for_area(area_name):
        raise NotImplemented()
