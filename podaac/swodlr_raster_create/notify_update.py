'''Lambda which sends each job in the jobset as a message to a SNS topic'''
import json
import boto3
from mypy_boto3_sns import SNSClient
from .utils import get_logger, get_param, load_json_schema

MAX_ATTEMPTS = int(get_param('update_max_attempts'))
UPDATE_TOPIC_ARN = get_param('update_topic_arn')

logger = get_logger(__name__)

sns: SNSClient = boto3.client('sns')
validate_jobset = load_json_schema('jobset')


def lambda_handler(event, _context):
    '''
    Lambda handler which sends each job in a JobSet as a message to a SNS topic
    '''
    jobset = validate_jobset(event)
    msg_queue = {}

    for job in jobset['jobs']:
        message = {
            'Id': job['product_id'],
            'Message': json.dumps(job, separators=(',', ':'))
        }
        msg_queue[job['product_id']] = message

    for i in range(1, MAX_ATTEMPTS + 1):
        logger.debug('Sending updates; attempt %d/%d', i, MAX_ATTEMPTS)
        res = sns.publish_batch(
            TopicArn=UPDATE_TOPIC_ARN,
            PublishBatchRequestEntries=list(msg_queue.values())
        )

        for message in res['Successful']:
            del msg_queue[message['Id']]

        for message in res['Failed']:
            logger.error(
                'Failed to send update: id: %s, code: %s, message: %s',
                message['Id'], message['Code'], message.get('Message', '-')
            )

            if message['SenderFault']:
                # Sending again won't fix this issue
                del msg_queue[message['Id']]

        # Warn when remaining after attempt - not a fail state yet
        if len(msg_queue) > 0:
            logger.warning('Remaining messages in queue: %d', len(msg_queue))
        else:
            break

    # Max attempts reached - fail state
    if len(msg_queue) > 0:
        raise RuntimeError(f'Failed to send {len(msg_queue)} update messages')

    return jobset
