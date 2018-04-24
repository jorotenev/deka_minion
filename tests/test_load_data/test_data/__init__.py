import json

from shared_utils.file_utils import get_directory_of_file

dummy_data = json.load(open("%s/sofia_short.json" % get_directory_of_file(__file__)))
