import os

_current_dir = os.path.dirname(os.path.realpath(__file__))


class Config:
    raw_input_file = "%s/coords.json" % _current_dir
    google_access_key = os.environ['GOOGLE_ACCESS_KEY']
    google_places_api_url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    output_folder = "output"
