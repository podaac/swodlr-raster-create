from copy import deepcopy
import json
import logging
from .utils import update_queue, load_json_schema, get_param

MAX_ATTEMPTS = int(get_param('update_queue_max_attempts'))

validate_jobset = load_json_schema('jobset')

def lambda_handler(event, _context):
  jobset = validate_jobset(event)
  msg_queue = {}

  # Generate message queue
  for job in jobset['jobs']:
    message = {
      'Id': job['product_id'],
      'MessageBody': json.dumps(job)
    }
    msg_queue[job['product_id']] = message

  # Send updates to SQS
  for i in range(1, MAX_ATTEMPTS + 1):
    logging.debug('Sending updates; attempt %d/%d', i, MAX_ATTEMPTS)
    res = update_queue.send_messages(
      Entries=list(msg_queue.values())
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
