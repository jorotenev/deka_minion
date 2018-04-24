from collections import namedtuple

# leaflet inspired :)
LatLng = namedtuple("LatLng", ['lat', 'lng'])
# the values are LatLngs
GeoRectangle = namedtuple("GeoRectangle", ['northwest', 'southeast'])
Metadata = namedtuple("Metadata", ['area_name', 'bounding_rectangle'])
