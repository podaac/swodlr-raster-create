import json
import logging
from podaac.swodlr_raster_create.utils import (
    mozart_client, get_param, search_datasets, load_json_schema
)

DATASET_NAME    = 'SWOT_L2_HR_PIXCVec'
PCM_RELEASE_TAG = get_param('sds_pcm_release_tag')

validate_input = load_json_schema('input')

raster_eval_job_type = mozart_client.get_job_type(
    f'job-SUBMIT_L2_HR_Raster:{PCM_RELEASE_TAG}'
)
raster_eval_job_type.initialize()

def lambda_handler(event, _context):
    logging.debug('Records received: %d', len(event['Records']))

    jobs = {}
    for record in event['Records']:
        job = _process_record(record)
        jobs[job['id']] = job

    return {'jobs': jobs}

def _process_record(record):
    body = validate_input(json.loads(record['body']))

    cycle = body['cycle']
    passe = body['pass']
    scene = body['scene']
    tile = _scene_to_tile(body['scene'])  # Josh: 3:<

    pixcvec_granule_name = f'{DATASET_NAME}_{cycle}_{passe}_{tile}_*'
    granule = search_datasets(pixcvec_granule_name)

    if granule is None:
        return None

    raster_eval_job_type.set_input_dataset(granule)
    job = raster_eval_job_type.submit_job('raster_evaluator_otello_submit')
    logging.info(
        'Submitted - job id: %s, cycle: %d, pass: %d, scene: %s',
        job.job_id, cycle, passe, scene
    )

    return {
        'id': job.job_id,
        'product_id': body['product_id'],
        'status': 'job-queued',
        'metadata': {
            'cycle': cycle,
            'pass': passe,
            'scene': scene,
            'output_granule_extent_flag': body['output_granule_extent_flag'],
            'output_sampling_grid_type': body['output_sampling_grid_type'],
            'raster_resolution': body['raster_resolution'],
            'utm_zone_adjust': body.get('utm_zone_adjust', 0),
            'mgrs_band_adjust': body.get('mgrs_band_adjust', 0)
        }
    }


def _scene_to_tile(scene_id):
    '''
    Converts a scene id to the first tile id in the set
    TODO: REMOVE THIS ONCE THE SDS ACCEPTS EXPLICIT SCENE IDS
    '''
    return f'{(scene_id * 2) - 2}L'
