'''Lambda which publishes resulting SDS data to a S3 bucket'''
from os.path import join as joinpath
from pathlib import PurePath
from urllib.parse import urlunsplit, urlparse
import boto3
from mypy_boto3_s3 import S3Client
from . import sds_statuses
from .utils import get_logger, get_param, mozart_client, load_json_schema

ACCEPTED_EXTS = {'nc'}
PUBLISH_BUCKET = get_param('publish_bucket')

s3: S3Client = boto3.client('s3')
validate_jobset = load_json_schema('jobset')
logger = get_logger(__name__)


def lambda_handler(event, _context):
    '''
    Lambda handler that takes an input jobset, retrieves a job's products,
    searches through the products' buckets for accepted files by file
    extension, and copies the files from the SDS bucket to the publication
    bucket
    '''
    jobset = validate_jobset(event)

    for job in jobset['jobs']:
        if job['job_status'] not in sds_statuses.SUCCESS:
            continue

        mozart_job = mozart_client.get_job_by_id(job['job_id'])
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

    jobset = validate_jobset(jobset)
    return jobset


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
