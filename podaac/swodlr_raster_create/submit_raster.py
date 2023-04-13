import logging
from . import sds_statuses
from .utils import (
  get_param, mozart_client, load_json_schema, search_datasets
)

STAGE           = __name__.rsplit('.', 1)[1]
PCM_RELEASE_TAG = get_param('sds_pcm_release_tag')

validate_jobset = load_json_schema('jobset')
raster_job_type = mozart_client.get_job_type(
  f'job-SCIFLO_L2_HR_Raster:${PCM_RELEASE_TAG}'
)
raster_job_type.initialize()


def lambda_handler(event, _context):
    input_jobset = validate_jobset(event)

    raster_jobs = [process_job(eval_job) for eval_job in input_jobset['jobs']]

    output = validate_jobset({'jobs': raster_jobs})
    return output

def process_job(eval_job):
    if eval_job['job_status'] not in sds_statuses.SUCCESS:
        # Pass through fail statuses
        return eval_job

    raster_job = {
        'stage': STAGE,
        'product_id': eval_job['product_id']
    }

    state_config_id = mozart_client        \
        .get_job_by_id(eval_job['job_id'])     \
        .get_generated_products()[0]['id']

    state_config = search_datasets(state_config_id, False)
    if state_config is None:
      raster_job.update(
        job_status='job-failed',
        errors=['Unable to find state config from submit_evaluate stage']
      )
      return raster_job

    params = [
      'raster_resolution', 'output_sampling_grid_type',
      'output_granule_extent_flag', 'utm_zone_adjust', 'mgrs_band_adjust'
    ]
    input_params = {param: eval_job['metadata'][param] for param in params}

    # Input param conversions
    input_params['output_sampling_grid_type'] = \
      input_params['output_sampling_grid_type'].lower()
    input_params['output_granule_extent_flag'] = \
      1 if input_params['output_granule_extent_flag'] else 0

    raster_job_type.set_input_dataset(state_config)
    raster_job_type.set_input_params(input_params)
    sds_job = raster_job_type.submit_job(tag='sciflo_raster_otello_submit')

    raster_job.update(
        job_id = sds_job.job_id,
        job_status = 'job-queued',
        metadata = eval_job['metadata']
    )
    return raster_job
