'''Lambda which publishes resulting SDS data to a S3 bucket'''
from os.path import join as joinpath
from pathlib import PurePath
from urllib.parse import urlunsplit, urlparse
import boto3
from mypy_boto3_s3 import S3Client
from podaac.swodlr_common import sds_statuses
from podaac.swodlr_common.decorators import job_handler
from .utilities import utils

ACCEPTED_EXTS = {'nc'}
PUBLISH_BUCKET = utils.get_param('publish_bucket')

s3: S3Client = boto3.client('s3')
logger = utils.get_logger(__name__)


@job_handler
def handle_job(job):
    if job['job_status'] not in sds_statuses.SUCCESS:
        return job

    mozart_job = utils.mozart_client.get_job_by_id(job['job_id'])
    products = mozart_job.get_generated_products()

    granules = _find_granules(products)
    s3_urls = []

    logger.debug(
        'Extracted granules (%s): %s',
        job['product_id'], granules
    )

    for granule in granules:
        key = joinpath(granule['collection'], granule['filename'])
        logger.debug(
            'Key (%s): %s',
            job['product_id'], key
        )

        logger.debug('Bucket: %s', PUBLISH_BUCKET)
        logger.info('Upload starting: %s', granule['filename'])
        s3.copy(
            CopySource=granule['source'],
            Bucket=PUBLISH_BUCKET,
            Key=key
        )
        logger.info('Upload finished: %s', granule['filename'])

        url = urlunsplit(('s3', PUBLISH_BUCKET, key, '', ''))
        s3_urls.append(url)

        job['granules'] = s3_urls
        return job


def _find_granules(products):
    granules = []
    for product in products:
        for url in product['urls']:
            parsed_url = urlparse(url)
            if parsed_url.scheme != 's3':
                continue

            sds_path = PurePath(parsed_url.path)
            sds_bucket = sds_path.parts[1]
            sds_prefix = joinpath(*sds_path.parts[2:])

            objects = s3.list_objects_v2(
                Bucket=sds_bucket,
                Prefix=sds_prefix
            )['Contents']

            for obj in objects:
                obj_path = PurePath(obj['Key'])
                if obj_path.suffix[1:].lower() in ACCEPTED_EXTS:
                    granules.append({
                        'collection': product['dataset'],
                        'filename': obj_path.name,
                        'source': {
                            'Bucket': sds_bucket,
                            'Key': str(obj_path)
                        }
                    })

    return granules
