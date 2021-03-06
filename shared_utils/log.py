import logging as log
from datetime import datetime as dt
from os import environ
from shared_utils.file_utils import touch_directory


def init_logger(output_dir):
    log_dir = "%s/%s" % (output_dir, 'log')
    touch_directory(log_dir)

    log.basicConfig(
        level=getattr(log, environ.get("LOG_LEVEL", "INFO")),
        format='[%(levelname)s::%(asctime)s] %(message)s',
        handlers=[
            log.FileHandler("{0}/{1}.log".format(log_dir, 'deka_minion_%s' % dt.now().isoformat().replace(":", "_"))),
            log.StreamHandler()
        ]
    )
