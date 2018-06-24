import os


class Config:
    google_access_key = os.environ['DEKA_GOOGLE_ACCESS_KEY']
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
