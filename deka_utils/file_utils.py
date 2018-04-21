import json
from os import path, makedirs
import logging as log
from datetime import datetime as dt

from deka_config import Config


def readJSONFileAndConvertToDict(filepath):
    return json.load(open(filepath))


def save_places_to_file(places):
    file_path = "{folder}/{num_places}_{date}.json".format(
        folder=Config.output_folder,
        num_places=len(places),
        date=dt.now().isoformat()
    )
    abs_file_path = path.abspath(file_path)
    touch_directory(path.dirname(abs_file_path))

    log.info("Saving %i places to %s" % (len(places), file_path))
    with(open(abs_file_path, 'w')) as file:
        file.write(json.dumps(places))


def touch_directory(dir_path):
    """
    creates a dir if it doesn't exists
    """
    if not path.exists(dir_path):
        makedirs(dir_path)
