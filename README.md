# deka_minion
Service for https://github.com/jorotenev/deka that fetches the venues data and loads it to a shared datastore.


# run.sh
* `$ pip install pipenv`
* `$ export DEKA_GOOGLE_ACCESS_KEY=<key>`
* `$ pipenv --python=3.6 && pipenv install`
* `/bin/bash run.sh  $(realpath get_places/input/copy.json)`