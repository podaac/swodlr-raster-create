'''
Lambda which retrieves the job statuses from the SDS and updates the waiting
flag
'''
import json
from podaac.swodlr_common.decorators import bulk_job_handler
from podaac.swodlr_common.logging import JobMetadataInjector
from podaac.swodlr_common import sds_statuses
from .utilities import utils


logger = utils.get_logger(__name__)
validate_jobset = utils.load_json_schema('jobset')


@bulk_job_handler(returns_jobset=True)
def handle_jobs(jobset):
    '''
    Lambda handler which accepts a jobset, updates the statuses from the SDS,
    appends a waiting flag if the jobset still has jobs that haven't completed,
    and returns the updated jobset
    '''
    waiting = False

    for job in jobset['jobs']:
        job_logger = JobMetadataInjector(logger, job)

        if job['job_status'] not in sds_statuses.WAITING:
            job_logger.debug('Skipping job; status: %s', job['job_status'])
            continue

        job_id = job['job_id']
        try:
            job_info = utils.mozart_client.get_job_by_id(job_id).get_info()
        except Exception:  # pylint: disable=broad-exception-caught
            job_logger.exception('Failed to get job info')
            waiting = True
            continue

        job_status = job_info['status']
        if job_status == 'job-offline' and 'timedout' in job_info['tags']:
            job_status = 'job-timedout'  # Custom Swodlr status

        if job_status in sds_statuses.WAITING:
            job_logger.info('Waiting for job; status: %s', job_status)
            waiting = True
        else:
            job_logger.info('Job finished; status: %s', job_status)

            job_logger.debug('Pulling metrics out')
            metrics = _extract_metrics(job_info)
            job_logger.info('SDS metrics: %s', json.dumps({
                'metrics': metrics,
                'input': jobset['inputs'][job['product_id']]
            }))

        job['job_status'] = job_status  # Update job in JobSet

        if 'traceback' in job_info:
            job.update(
                traceback=job_info['traceback'],
                errors=['SDS threw an error. Please contact support']
            )

    if waiting:
        jobset['waiting'] = True
    elif 'waiting' in jobset:
        del jobset['waiting']

    output = validate_jobset(jobset)
    return output


def _extract_metrics(job):
    metric_keys = ('time_queued', 'time_start', 'time_end')
    metrics = {key: job['job']['job_info'].get(key) for key in metric_keys}
    return metrics
