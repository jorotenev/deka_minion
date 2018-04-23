# deka_minion
## get_data
This package is responsible for querying the Google Places Search API.
The input is a set of coordinates-radius pairs (which define a geographical area). The package then queries the API
for each of these areas and collects all the places within them.

There are some quirks about the Google Place API. Notably, for a single query (i.e. lat,lng & radius) the API will
return at most 60 results. The Google API allows us to specify only **one** criteria for venue type (i.e. return
only "restaurants"). However, we are interested in more venue types (~10).
The wrapper will first attempt to query a single area and if the results set has 60 items, it's highly likely that
there are more places within the area and the result is capped as per the API limitation of max 60 items. This wrapper
handles this problem by first querying the area without any filters to get all places, and if the result set is with 60 places,
query again sequentially for all place types we are interested in (so if we care about ten types, ten queries for the same
geographical areas are made). Given that this happens not often (e.g. for Sofia, with ~5000 areas to query, ~500 of them
need to be queried more thouroughy due to the limit of 60).


The package outputs a single file which contains all of the places from the input areas.

**Format of the input:**
```
{
    "coordinates" : [{"lat":11, "lng":22}, ....],
    "circle_radius": 150 // metres. all areas have the same radius.
}
```

**The output file format is:**
```javascript
{
    "<place_id>" : {
        "<place_attr_1>": "<place_attr_1_value>",
        ...
    },
    <another_place_id>: {...}
}
```