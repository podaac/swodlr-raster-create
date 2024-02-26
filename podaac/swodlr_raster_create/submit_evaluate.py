'''
Lambda which processes the SQS message for inputs, submits the job(s) to the
SDS, and returns a jobset
'''
from copy import deepcopy
from time import sleep

from podaac.swodlr_common.decorators import bulk_job_handler
from requests import RequestException

from .utilities import utils

STAGE = __name__.rsplit('.', 1)[1]
DATASET_NAME = 'SWOT_L2_HR_PIXCVec'
PCM_RELEASE_TAG = utils.get_param('sds_pcm_release_tag')
MAX_ATTEMPTS = int(utils.get_param('sds_submit_max_attempts'))
TIMEOUT = int(utils.get_param('sds_submit_timeout'))

logger = utils.get_logger(__name__)
validate_jobset = utils.load_json_schema('jobset')

raster_eval_job_type = utils.mozart_client.get_job_type(
    utils.get_latest_job_version('job-SUBMIT_L2_HR_Raster')
)
raster_eval_job_type.initialize()


@bulk_job_handler(returns_jobset=True)
def bulk_job_handler(jobset):
    '''
    Lambda handler which accepts an SQS message, parses records as inputs,
    submits jobs to the SDS, and returns a jobset
    '''
    inputs = deepcopy(jobset['inputs'])
    jobs = [_process_input(input_) for input_ in jobset['inputs'].values()]

    job_set = validate_jobset({
        'jobs': jobs,
        'inputs': inputs
    })
    return job_set


def _process_input(input_):
    output = {
        'stage': STAGE,
        'product_id': input_['product_id']
    }

    cycle = str(input_['cycle']).rjust(3, '0')
    passe = str(input_['pass']).rjust(3, '0')
    tile = _scene_to_tile(input_['scene'])  # Josh: 3:<

    pixcvec_granule_name = f'{DATASET_NAME}_{cycle}_{passe}_{tile}_*'

    try:
        granule = utils.search_datasets(pixcvec_granule_name)
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
                job_status='job-queued'
            )
            return output
        # pylint: disable=duplicate-code
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception(
                'Job submission failed - attempt %d/%d; product_id=%s',
                i, MAX_ATTEMPTS, output['product_id']
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
