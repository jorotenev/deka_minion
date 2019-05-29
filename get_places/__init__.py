from dotenv import load_dotenv
load_dotenv()

import os
from shared_utils.log import init_logger
package_dir = os.path.dirname(os.path.realpath(__file__))
init_logger(package_dir)
