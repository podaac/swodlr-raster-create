'''
Lambda which accepts a evaluate jobset, utilizes the generated config from the
evaluate job, submits a raster job to the SDS, and outputs a new jobset
consisting of raster jobs
'''
from time import sleep

from requests import RequestException
from podaac.swodlr_common.decorators import job_handler
from podaac.swodlr_common import sds_statuses

from .utilities import utils

STAGE = __name__.rsplit('.', 1)[1]
PCM_RELEASE_TAG = utils.get_param('sds_pcm_release_tag')
MAX_ATTEMPTS = int(utils.get_param('sds_submit_max_attempts'))
TIMEOUT = int(utils.get_param('sds_submit_timeout'))

logger = utils.get_logger(__name__)

validate_jobset = utils.load_json_schema('jobset')
raster_job_type = utils.mozart_client.get_job_type(
    f'job-SCIFLO_L2_HR_Raster:{PCM_RELEASE_TAG}'
)
raster_job_type.initialize()

@job_handler
def handle_job(eval_job):
    if eval_job['job_status'] not in sds_statuses.SUCCESS:
        # Pass through fail statuses
        return eval_job

    raster_job = {
        'stage': STAGE,
        'product_id': eval_job['product_id'],
        'metadata': eval_job['metadata']
    }

    cycle = str(raster_job['metadata']['cycle']).rjust(3, '0')
    passe = str(raster_job['metadata']['pass']).rjust(3, '0')
    scene = str(raster_job['metadata']['scene']).rjust(3, '0')
    state_config_id = f'L2_HR_Raster_{cycle}_{passe}_{scene}-state-config'

    try:
        state_config = utils.search_datasets(state_config_id, False)
    except RequestException:
        logger.exception('ES request failed')
        raster_job.update(
            job_status='job-failed',
            errors=['ES request failed']
        )
        return raster_job

    if state_config is None:
        logger.error('State config is missing: %s', state_config_id)
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
            logger.exception(
                'Job submission failed; attempt %d/%d',
                i, MAX_ATTEMPTS
            )

        sleep(TIMEOUT)

    raster_job.update(
        job_status='job-failed',
        errors=['SDS failed to accept job']
    )
    return raster_job
