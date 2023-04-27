'''
Lambda which accepts a evaluate jobset, utilizes the generated config from the
evaluate job, submits a raster job to the SDS, and outputs a new jobset
consisting of raster jobs
'''
import logging
from time import sleep

from requests import RequestException
from . import sds_statuses
from .utils import (
    get_param, mozart_client, load_json_schema, search_datasets
)

STAGE = __name__.rsplit('.', 1)[1]
PCM_RELEASE_TAG = get_param('sds_pcm_release_tag')
MAX_ATTEMPTS = int(get_param('sds_submit_max_attempts'))
TIMEOUT = int(get_param('sds_submit_timeout'))

validate_jobset = load_json_schema('jobset')
raster_job_type = mozart_client.get_job_type(
    f'job-SCIFLO_L2_HR_Raster:{PCM_RELEASE_TAG}'
)
raster_job_type.initialize()


def lambda_handler(event, _context):
    '''
    Lambda handler which accepts an evaluate jobset, retrieves the
    configuration from the evaluate jobs, submits the raster jobs, and outputs
    a new raster jobset
    '''
    input_jobset = validate_jobset(event)

    raster_jobs = [process_job(eval_job) for eval_job in input_jobset['jobs']]

    output = validate_jobset({'jobs': raster_jobs})
    return output


def process_job(eval_job):
    '''
    Takes a evaluate job from a jobset, retrieves the config from GRQ
    ElasticSearch on the SDS, submits a raster job, and returns the raster job
    object
    '''
    if eval_job['job_status'] not in sds_statuses.SUCCESS:
        # Pass through fail statuses
        return eval_job

    raster_job = {
        'stage': STAGE,
        'product_id': eval_job['product_id'],
        'metadata': eval_job['metadata']
    }

    state_config_id = mozart_client        \
        .get_job_by_id(eval_job['job_id']) \
        .get_generated_products()[0]['id']

    try:
        state_config = search_datasets(state_config_id, False)
    except RequestException:
        logging.exception('ES request failed')
        raster_job.update(
            job_status='job-failed',
            errors=['ES request failed']
        )
        return raster_job

    if state_config is None:
        logging.error('State config is missing: %s', state_config_id)
        raster_job.update(
            job_status='job-failed',
            errors=['Unable to find state config from submit_evaluate stage']
        )
        return raster_job

    passthru_params = [
        'raster_resolution', 'output_sampling_grid_type',
        'output_granule_extent_flag', 'utm_zone_adjust', 'mgrs_band_adjust'
    ]
    input_params = {
        param: eval_job['metadata'][param]
        for param in passthru_params
    }

    # Input param conversions
    input_params['output_sampling_grid_type'] = \
        input_params['output_sampling_grid_type'].lower()
    input_params['output_granule_extent_flag'] = \
        1 if input_params['output_granule_extent_flag'] else 0

    raster_job_type.set_input_dataset(state_config)
    raster_job_type.set_input_params(input_params)

    for i in range(1, MAX_ATTEMPTS + 1):
        try:
            sds_job = raster_job_type.submit_job(
                tag='sciflo_raster_otello_submit'
            )

            raster_job.update(
                job_id=sds_job.job_id,
                job_status='job-queued'
            )
            return raster_job
        # pylint: disable=duplicate-code
        except Exception:  # pylint: disable=broad-exception-caught
            logging.exception(
                'Job submission failed; attempt %d/%d',
                i, MAX_ATTEMPTS
            )

        sleep(TIMEOUT)

    raster_job.update(
        job_status='job-failed',
        errors=['SDS failed to accept job']
    )
    return raster_job
