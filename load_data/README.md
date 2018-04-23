# deka_minion
## laod_data

This package is responsible for loading a set of places within a large geographical area (e.g. a city) into
a datastore.

The datastore is optimized for querying geo data - Redis in our case.

The input of this package is a file, as outputted from the `get_places` package - see its README for example of the format
of the file.

