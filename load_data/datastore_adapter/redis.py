"""
redis-py docs https://github.com/andymccurdy/redis-py
"""
import json
import logging as log
from typing import Dict

from redis import StrictRedis

from load_data.config import Config
# instance of the redis client
from load_data.deka_types import Metadata, LatLng

r = StrictRedis(host=Config.REDIS_HOST, port=Config.REDIST_PORT, db=Config.REDIS_DB, decode_responses=True)
cities_boundaries_template_key = "cities:boundaries:"
# e.g. cities:places:london
cities_places_template_key = "cities:places:"
# e.g. cities:coordinates:london
cities_coordinates_template_key = "cities:coordinates:"


class KeyConverter:
    temp_stage_suffix = ":temporarykey"

    @classmethod
    def to_temp(cls, k):
        return k + cls.temp_stage_suffix

    @classmethod
    def from_temp(cls, k):
        return k.split(cls.temp_stage_suffix)[0]

    @classmethod
    def get_area_name(cls, key):
        # "cities:places:london"
        return key.split(":")[-1]


def load_to_datastore(places: Dict, metadata: Metadata):
    """

    :param places: dict. the keys are place_id (as per Google Places Search API). The value is a place object, as returned by
    the same API.
    :param metadata:
    :return boolean - True if successfully added
    """
    try:
        log.info("Begin the process of loading the new data.")
        # add all data to temporary keys
        load_to_temporary(places, metadata)

        log.info("Begin promoting the new data.")
        # delete the old data and promote the stand-by data to official
        promote_temp_to_official(metadata.area_name)
        log.info("%i places were successfully promoted & available for the [%s] area"
                 % (len(places), metadata.area_name))
        return True
    except:
        return False


def load_to_temporary(places, metadata):
    """
    load all of the data to temporary keys.

    :param places:
    :param metadata:
    :return:
    """
    boundaries_rectangle = metadata.bounding_rectangle
    area_name = metadata.area_name
    temp_area_name = KeyConverter.to_temp(area_name)
    transaction = r.pipeline()

    Facade.add_boundaries(area_name=temp_area_name,
                          boundaries_rectangle=boundaries_rectangle,
                          pipe=transaction)
    Facade.add_places(area_name=temp_area_name, places=places, pipe=transaction)
    Facade.add_coordinates(area_name=temp_area_name, places=places, pipe=transaction)
    try:
        transaction.execute(raise_on_error=True)
        log.info("Added new data in a temporary stage [%s]" % area_name)

        return True
    except:
        return False


def promote_temp_to_official(area_name):
    temp_name = KeyConverter.to_temp(area_name)
    transaction = r.pipeline()

    # delete the old keys
    transaction.delete(
        cities_boundaries_template_key + area_name,
        cities_places_template_key + area_name,
        cities_coordinates_template_key + area_name
    )
    # promote the new data to 'official' stage

    templates = [cities_boundaries_template_key, cities_coordinates_template_key, cities_places_template_key]
    for template_key in templates:
        transaction.rename(template_key + temp_name, template_key + area_name)

    try:
        transaction.execute(raise_on_error=True)
    except:
        return False


def deserialize(serialized):
    return json.loads(serialized)


def serialize(raw):
    return json.dumps(raw)


def extract_latlng_of_place(place) -> LatLng:
    loc = place['geometry']['location']
    return LatLng(lat=loc['lat'], lng=loc['lng'])


class Facade:
    def __init__(self):
        pass

    @classmethod
    def add_boundaries(cls, area_name, boundaries_rectangle, pipe):
        """
        :param area_name: e.g. "london"
        :param boundaries_rectangle: {"southeast":{"lat":1, "lng":1}, "northwest": {...}}
        :param pipe - https://github.com/andymccurdy/redis-py#pipelines  we execute this command
        only as part of a transaction.
        :return: boolean
        """
        return pipe.set(cities_boundaries_template_key + area_name, serialize(boundaries_rectangle))

    @classmethod
    def add_places(cls, area_name, places: Dict, pipe):
        """
        Add the places to our datastore. Places is a dict with key=<a place_id> and value a place object,
        as returned by the Google Places API
        :param area_name: e.g. london
        :param places: dict with place_id-> place pairs
        :param pipe:
        :return:
        """
        for place_id, place in places.items():
            pipe.hset(cities_places_template_key + area_name, place_id, serialize(place))
        return pipe

    @classmethod
    def add_coordinates(cls, area_name, places: Dict, pipe):
        """
        Add the coordinates of each place to a geo-optimized key (in a data structure provided by redis).
        We add [longitute, latutude, "<place_id>"] for each place
        :param area_name:
        :param places:
        :param pipe:
        :return:
        """
        for place_id, place in places.items():
            lat_lng = extract_latlng_of_place(place)

            pipe.geoadd(cities_coordinates_template_key + area_name, lat_lng.lng, lat_lng.lat, place_id)
        return pipe

    @classmethod
    def get_boundaries_for_area(cls, area_name):
        raw = r.get(cities_boundaries_template_key + area_name)
        return deserialize(raw)

    @classmethod
    def get_place_data(cls, area_name, place_key):
        raw = r.hget(cities_places_template_key + area_name, place_key)
        return deserialize(raw)

    @classmethod
    def get_all_places_for_area(cls, area_name):
        raise NotImplemented()
