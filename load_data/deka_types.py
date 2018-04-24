from collections import namedtuple

# leaflet inspired :)
LatLng = namedtuple("LatLng", ['lat', 'lng'])
# the values are LatLngs

Metadata = namedtuple("Metadata", ['area_name', 'bounding_rectangle'])
