import os

from shared_utils.file_utils import get_durectiry_of_file


class Config:
    raw_input_file = "%s/input/coords.json" % get_durectiry_of_file(__file__)
    google_access_key = os.environ['GOOGLE_ACCESS_KEY']
    google_places_api_url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    output_folder = "output"

    """
    The type of venues that we're interested in. 
    Google allows us to query with only one type of places.
    Thus, we query for all types, and then filter.  
    """
    places_types = [
        "bakery",
        "bar",
        "cafe",
        "casino",
        "department_store",
        "meal_takeaway",
        "movie_theater",
        "museum",
        "night_club",
        "park",
        "restaurant",
        "shopping_mall",
    ]
