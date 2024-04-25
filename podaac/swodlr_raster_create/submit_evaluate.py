'''
Lambda which processes the SQS message for inputs, submits the job(s) to the
SDS, and returns a jobset
'''
from copy import deepcopy
from time import sleep

from requests import RequestException

from podaac.swodlr_common.decorators import bulk_job_handler
from .utilities import utils

STAGE = __name__.rsplit('.', 1)[1]
DATASET_NAME = 'SWOT_L2_HR_PIXCVec'
PCM_RELEASE_TAG = utils.get_param('sds_pcm_release_tag')
MAX_ATTEMPTS = int(utils.get_param('sds_submit_max_attempts'))
TIMEOUT = int(utils.get_param('sds_submit_timeout'))

grq_es_client = utils.get_grq_es_client()

logger = utils.get_logger(__name__)
validate_jobset = utils.load_json_schema('jobset')

raster_eval_job_type = utils.mozart_client.get_job_type(
    utils.get_latest_job_version('job-SUBMIT_L2_HR_Raster')
)
raster_eval_job_type.initialize()


@bulk_job_handler(returns_jobset=True)
def handle_bulk_job(jobset):
    '''
    Lambda handler which accepts an SQS message, parses records as inputs,
    submits jobs to the SDS, and returns a jobset
    '''
    inputs = deepcopy(jobset['inputs'])
    jobs = [_process_input(input_) for input_ in jobset['inputs'].values()]

    job_set = {
        'jobs': jobs,
        'inputs': inputs
    }
    return job_set


def _process_input(input_):
    output = {
        'stage': STAGE,
        'product_id': input_['product_id']
    }

    cycle = input_['cycle']
    passe = input_['pass']
    scene = input_['scene']

    # 3: - Josh
    tiles = [
        str(f'{tile:03}') for tile in range((scene * 2) - 1, (scene * 2) + 1)
    ]

    try:
        results = grq_es_client.search(
            index='grq',
            size=10,
            body={
                'query': {
                    'bool': {
                        'must': [
                            {'term': {'dataset_type.keyword': 'SDP'}},
                            {'term': {'dataset.keyword': 'L2_HR_PIXCVec'}},
                            {'term': {'metadata.CycleID': f'{cycle:03}'}},
                            {'term': {'metadata.PassID': f'{passe:03}'}},
                            {'terms': {'metadata.TileID': tiles}}
                        ]
                    }
                }
            }
        )
    except RequestException:
        logger.exception('ES request failed')
        output.update(
            job_status='job-failed',
            errors=['ES request failed']
        )
        return output

    hits = results['hits']['hits']

    if len(hits) == 0:
        logger.error(
            'ES search returned no results - cycle: %d, pass: %d, scene: %d',
            cycle, passe, scene
        )
        output.update(
            job_status='job-failed',
            errors=['Scene does not exist']
        )
        return output

    raster_eval_job_type.set_input_dataset(hits[0])

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
