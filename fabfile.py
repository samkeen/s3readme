import logging
import os
from logging.config import fileConfig

from fabric.api import *

from s3readme.s3_adapter import S3Adapter

this_dir = os.path.dirname(os.path.realpath(__file__))
log_config_path = os.path.join(this_dir, 'logging_config.ini')
if not os.path.isfile(log_config_path):
    raise RuntimeError('Log config does not exist at path: {}'.format(log_config_path))
fileConfig(log_config_path)
logger = logging.getLogger()


@task()
def init(aws_region, bucket, prefix=''):
    """
    :return:
    """

    s3 = S3Adapter(aws_region, os.path.join(this_dir, 'config.yaml'), logger)
    print(s3.get_paths(aws_region, bucket, prefix))


@task()
def write():
    """
    Initially we will simply overwrite what is in s3 with what we have locally
    :return:
    """
    pass
