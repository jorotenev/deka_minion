import sys
from os.path import basename

from boto3 import session

s3 = session.Session().client('s3')

s3_bucket = sys.argv[1]
file_path = sys.argv[2]

s3.upload_file(Filename=file_path, Bucket=s3_bucket, Key=basename(file_path))
