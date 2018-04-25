import json

from shared_utils.file_utils import get_directory_of_file

dummy_data_sofia = json.load(open("%s/sofia.json" % get_directory_of_file(__file__)))
dummy_data_leuven = json.load(open("%s/leuven.json" % get_directory_of_file(__file__)))
