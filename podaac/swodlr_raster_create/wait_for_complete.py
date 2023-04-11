import logging
from . import sds_statuses
from .utils import mozart_client, load_schema

validate_jobset = load_schema('jobset')

def lambda_handler(event, _context):
  event = validate_jobset(event)
  jobs = event['jobs']
  waiting = False

  for job in event['jobs']:
    if job['status'] in sds_statuses.SUCCESS or sds_statuses.FAIL:
      logging.trace('Skipping %s; status: %s', job['id'], job['status'])
      continue

    job_id = job['job_id']
    job_info = mozart_client.get_job_by_id(job_id).get_info()
    job_status = job_info['status']
    job['status'] = job_status
  
    if 'traceback' in job_info:
      job.update(
        traceback = job_info['traceback'],
        errors = ['SDS threw an error']
      )
    elif job_status in sds_statuses.WAITING:
      logging.info('Waiting on %s', job['id'])
      waiting = True

  output = {'jobs': jobs}
  if waiting:
    output.update(waiting = waiting)

  output = validate_jobset(output)
  return output
