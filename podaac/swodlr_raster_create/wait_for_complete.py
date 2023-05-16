'''
Lambda which retrieves the job statuses from the SDS and updates the waiting
flag
'''
from . import sds_statuses
from .utils import get_logger, mozart_client, load_json_schema


logger = get_logger(__name__)
validate_jobset = load_json_schema('jobset')


def lambda_handler(event, _context):
    '''
    Lambda handler which accepts a jobset, updates the statuses from the SDS,
    appends a waiting flag if the jobset still has jobs that haven't completed,
    and returns the updated jobset
    '''
    jobset = validate_jobset(event)
    jobs = jobset['jobs']
    waiting = False

    for job in jobs:
        if job['job_status'] not in sds_statuses.WAITING:
            logger.debug(
                'Skipping %s; status: %s', job['product_id'], job['job_status']
            )
            continue

        job_id = job['job_id']
        try:
            job_info = mozart_client.get_job_by_id(job_id).get_info()
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception('Failed to get job info: %s', job_id)
            waiting = True
            continue

        job_status = job_info['status']
        if job_status == 'job-offline' and 'timedout' in job_info['tags']:
            job_status = 'job-timedout'  # Custom Swodlr status

        job['job_status'] = job_status  # Update job in JobSet

        if 'traceback' in job_info:
            job.update(
                traceback=job_info['traceback'],
                errors=['SDS threw an error']
            )

        if job_status in sds_statuses.WAITING:
            logger.info('Waiting on %s', job['job_id'])
            waiting = True

    output = {'jobs': jobs}
    if waiting:
        output.update(waiting=waiting)

    output = validate_jobset(output)
    return output
