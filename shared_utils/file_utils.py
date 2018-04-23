import json
import os
from os import path, makedirs


def get_durectiry_of_file(file):
    return os.path.dirname(os.path.realpath(file))


def readJSONFileAndConvertToDict(filepath):
    return json.load(open(filepath))


def save_dict_to_file(data, file_path):
    abs_file_path = path.abspath(file_path)
    touch_directory(path.dirname(abs_file_path))

    with(open(abs_file_path, 'w')) as file:
        file.write(json.dumps(data))


def touch_directory(dir_path):
    """
    creates a dir if it doesn't exists
    """
    if not path.exists(dir_path):
        makedirs(dir_path)
