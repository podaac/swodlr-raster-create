import logging
from utils import (
  get_param, mozart_client, load_json_schema, search_datasets
)

PCM_RELEASE_TAG = get_param('sds_pcm_release_tag')

validate_jobset = load_json_schema('jobset')
raster_job_type = mozart_client.get_job_type(
  f'job-SCIFLO_L2_HR_Raster:${PCM_RELEASE_TAG}'
)
raster_job_type.initialize()

def lambda_handler(event, _context):
  evaluate_jobs = validate_jobset(event)
  raster_jobs = {}

  for eval_job in evaluate_jobs.values():
    state_config_id = mozart_client        \
        .get_job_by_id(eval_job['id'])     \
        .get_generated_products()[0]['id']

    state_config = search_datasets(state_config_id, False)
    if state_config is None:
      raise RuntimeError('Unable to find state config; it should exist')

    params = [
      'raster_resolution', 'output_sampling_grid_type',
      'output_granule_extent_flag', 'utm_zone_adjust', 'mgrs_band_adjust'
    ]
    input_params = {param: eval_job['metadata'][param] for param in params}

    raster_job_type.set_input_dataset(state_config)
    raster_job_type.set_input_params(input_params)
    job = raster_job_type.submit_job(tag='sciflo_raster_otello_submit')

    raster_job = {
      'id': job['id'],
      'product_id': eval_job['product_id'],
      'status': 'job-queued',
      'metadata': eval_job['metadata']
    }
    raster_jobs[raster_job['id']] = validate_jobset(raster_jobs)
  
  return raster_jobs
