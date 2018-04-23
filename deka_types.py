from collections import namedtuple
from typing import Dict

# a geographical area, defined by its coordinates are radius.
Circle = namedtuple('Circle', ['lat', 'lng', 'radius'])

# a single result from a query to the Google Places Search API
# it's just a type alias for annotations, with no run-time meaning
Place = Dict
