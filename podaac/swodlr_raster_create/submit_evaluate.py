'''
Lambda which processes the SQS message for inputs, submits the job(s) to the
SDS, and returns a jobset
'''
import json
from time import sleep

from requests import RequestException
from .utils import (
    mozart_client, get_logger, get_param, search_datasets, load_json_schema
)

STAGE = __name__.rsplit('.', 1)[1]
DATASET_NAME = 'SWOT_L2_HR_PIXCVec'
PCM_RELEASE_TAG = get_param('sds_pcm_release_tag')
MAX_ATTEMPTS = int(get_param('sds_submit_max_attempts'))
TIMEOUT = int(get_param('sds_submit_timeout'))

logger = get_logger(__name__)

validate_input = load_json_schema('input')
validate_jobset = load_json_schema('jobset')

raster_eval_job_type = mozart_client.get_job_type(
    f'job-SUBMIT_L2_HR_Raster:{PCM_RELEASE_TAG}'
)
raster_eval_job_type.initialize()


def lambda_handler(event, _context):
    '''
    Lambda handler which accepts an SQS message, parses records as inputs,
    submits jobs to the SDS, and returns a jobset
    '''
    logger.debug('Records received: %d', len(event['Records']))

    jobs = [_process_record(record) for record in event['Records']]

    job_set = {'jobs': jobs}
    job_set = validate_jobset(job_set)
    return job_set


def _process_record(record):
    body = validate_input(json.loads(record['body']))
    output = {
        'stage': STAGE,
        'product_id': body['product_id']
    }

    cycle = str(body['cycle']).rjust(3, '0')
    passe = str(body['pass']).rjust(3, '0')
    tile = _scene_to_tile(body['scene'])  # Josh: 3:<

    pixcvec_granule_name = f'{DATASET_NAME}_{cycle}_{passe}_{tile}_*'

    try:
        granule = search_datasets(pixcvec_granule_name)
    except RequestException:
        logger.exception('ES request failed')
        output.update(
            job_status='job-failed',
            errors=['ES request failed']
        )
        return output

    if granule is None:
        logger.error(
            'ES search returned no results: %s',
            pixcvec_granule_name
        )
        output.update(
            job_status='job-failed',
            errors=['Scene does not exist']
        )
        return output

    raster_eval_job_type.set_input_dataset(granule)

    for i in range(1, MAX_ATTEMPTS + 1):
        try:
            job = raster_eval_job_type.submit_job(
                tag='raster_evaluator_otello_submit'
            )

            output.update(
                job_id=job.job_id,
                job_status='job-queued',
                metadata={
                    'cycle': body['cycle'],
                    'pass': body['pass'],
                    'scene': body['scene'],
                    'output_granule_extent_flag':
                        body['output_granule_extent_flag'],
                    'output_sampling_grid_type':
                        body['output_sampling_grid_type'],
                    'raster_resolution': body['raster_resolution'],
                    'utm_zone_adjust': body.get('utm_zone_adjust'),
                    'mgrs_band_adjust': body.get('mgrs_band_adjust')
                }
            )
            return output
        # pylint: disable=duplicate-code
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception(
                'Job submission failed; attempt %d/%d',
                i, MAX_ATTEMPTS
            )

        sleep(TIMEOUT)

    output.update(
        job_status='job-failed',
        errors=['SDS failed to accept job']
    )
    return output


def _scene_to_tile(scene_id):
    '''
    Converts a scene id to the first tile id in the set
    TODO: REMOVE THIS ONCE THE SDS ACCEPTS EXPLICIT SCENE IDS
    '''
    base = str(scene_id * 2).rjust(3, '0')
    return f'{base}L'
