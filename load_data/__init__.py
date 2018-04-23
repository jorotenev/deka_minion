from shared_utils.file_utils import get_durectiry_of_file

from shared_utils.log import init_logger

package_dir = get_durectiry_of_file(__file__)
init_logger(package_dir)
