from copy import deepcopy
import json
import logging
from utils import db_update_queue, load_json_schema, get_param

MAX_ATTEMPTS = get_param('queue_update_max_attempts')

validate_jobset = load_json_schema('jobset')

def lambda_handler(event, _context):
  event = validate_jobset(event)
  jobs = event['jobs']
  msg_queue = deepcopy(jobs)

  # Generate message queue
  for job in jobs.values():
    message = {
      'Id': job['id'],
      'MessageBody': json.dumps(job)
    }
    msg_queue[job['id']] = message

  # Send updates to SQS
  for i in range(MAX_ATTEMPTS):
    logging.debug('Sending updates; attempt %d/%d', i + 1, MAX_ATTEMPTS)
    res = db_update_queue.send_messages(
      Entries=msg_queue.values(),
    )

    for message in res['Successful']:
      del msg_queue[message['Id']]

    for message in res['Failed']:
      logging.error('Failed to send update: id: %s, code: %s, message: %s',
        message['Id'], message['Code'], message['Message']
      )

      if message['SenderFault']:
        # Sending again won't fix this issue
        del msg_queue[message['Id']]

    # Warn when remaining after attempt - not a fail state yet
    if len(msg_queue) > 0:
      logging.warn('Remaining messages in queue: %d', len(msg_queue))
  
  # Max attempts reached - fail state
  if len(msg_queue) > 0:
    raise RuntimeError('Failed to send %d update messages', len(msg_queue))
