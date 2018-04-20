from deka_config import Config
from typing import List

_ACCESS_CODE = Config.google_access_key
_endpoint_url = Config.google_places_api_url


class Circle:
    """
    Represents circle geographical area, defined by coordinates and a radius
    """

    def __init__(self, lat, lng, radius):
        self.lat = lat
        self.lng = lng
        self.radius = radius

    def __repr__(self):
        return "Circle(lat=%s lng=%s r=%s)" % (self.lat, self.lng, self.radius)


def query_batches(batches: List[List[Circle]]):
    """
    Given circle batches, query the Google Places API for information about the venues
    within the geographical circles.

    Wraps query_batch() by using multiprocessing to speed up things.

    :param batches: a list of batches. Each batch is a list of Circle objects

    :return: a single list with *all* places within the circles from the batches
    """
    return []


def _query_batch(batch: List[Circle]):
    """
    Queries a batch of areas using the Google Places API.
    An area is represented by a Circle object (lat,lng, radius)
    The result contains all venues found within all areas.

    :param batch:
    :return: a list of places, as returned by the Google Places API
    """
    # todo use multithreading here ?
    return [_query_single_circle(circle=circle) for circle in batch]


def _build_api_url(params):
    # todo
    return "{base_url}?".format(
        base_url=_endpoint_url
    )


def _query_single_circle(circle: Circle):
    """
    Given the coordinates of an area (defined by a Circle),
    query the Google Places API for a list of all venues there.
    This method handles the fact that multiple pages of results might be returned.

    The result contains all venues within the given area (i.e. the given Circle)

    :param circle:
    :return: list of place dicts. each dict contains information about a place.
    """

    return []
