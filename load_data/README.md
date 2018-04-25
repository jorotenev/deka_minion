# deka_minion
## laod_data

This package is responsible for loading a set of places from within a large geographical area (e.g. a city) into
a geo-optimised datastore - Redis in our case.

The input of this package is a file, as outputted from the `get_places` package. Example file-format:
```json
{
    "metadata" : {
        "bounding_rectangle": {"northwest":{"lat":1, "lng":2}, "southeast":{"lat":1, "lng":2}},
        "area_name":"london"
    },
    "places": {
        "<place_id>": {
            "geometry": {"location": {"lat":1, "lng":2}},
            "place_id": "<place_id>",
            "photos": ["some more":"keys and values"]
        },

    }
}
```

The package then would load the above data to Redis.
For a given geographical area (let's say `"sofia"`) we would load into redis the following keys:
* `cities:places:sofia` - holds a hash. The key is a `place_id` (as returned by the Google Places Search API). The
value is a json-string holding a json object representing a single place. A place has information like its geolocation,
name, place-type (bar, cafe, etc.).
* `cities:coordinates:sofia` - holds an index of the coordinates of all places. We add a `[<lat>, <lng>, <place_id>] triple
for each place within the geographical area for which we load data. This index enables us to later efficiently query
for all places within some area (given a point and radius). See  [GEOADD](https://redis.io/commands/geoadd)
* `cities:boundaries:sofia` - holds the `bounding_rectangle` from the input's `metadata`. a json object containing the coordinates of a geographical rectangle surrounding the geographical area for which we add data.
We store this data so that, given a query with a `[lat, lng, radius]` we can quickly determine for which region the request is for and if we have places-data for this region at all.

By default, we'd first load all the data under `"sofia_temp"` first, then delete any existing `"sofia"` keys and then
rename `"sofia_temp"` effectively promoting the temp, "cold", data to the in-use, "hot", one.