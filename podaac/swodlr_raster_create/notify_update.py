import json
import logging
import boto3
from mypy_boto3_sns import SNSClient
from .utils import get_param, load_json_schema

MAX_ATTEMPTS = int(get_param('update_max_attempts'))
UPDATE_TOPIC_ARN = get_param('update_topic_arn')

sns: SNSClient = boto3.client('sns')
validate_jobset = load_json_schema('jobset')

def lambda_handler(event, context):
    jobset = validate_jobset(event)
    msg_queue = {}

    for job in jobset['jobs']:
        message = {
            'Id': job['product_id'],
            'Message': json.dumps(job, separators=(',', ':'))
        }
        msg_queue[job['product_id']] = message

    for i in range(1, MAX_ATTEMPTS + 1):
        logging.debug('Sending updates; attempt %d/%d', i, MAX_ATTEMPTS)
        res = sns.publish_batch(
            TopicArn=UPDATE_TOPIC_ARN,
            PublishBatchRequestEntries=list(msg_queue.values())
        )

        for message in res['Successful']:
            del msg_queue[message['Id']]

        for message in res['Failed']:
            logging.error('Failed to send update: id: %s, code: %s, message: %s',
                message['Id'], message['Code'], message.get('Message', '-')
            )

            if message['SenderFault']:
                # Sending again won't fix this issue
                del msg_queue[message['Id']]

        # Warn when remaining after attempt - not a fail state yet
        if len(msg_queue) > 0:
            logging.warning('Remaining messages in queue: %d', len(msg_queue))
        else:
            break
    
    # Max attempts reached - fail state
    if len(msg_queue) > 0:
        raise RuntimeError('Failed to send %d update messages', len(msg_queue))
