'''Lambda to bootstrap step function execution'''
import json
import logging
import boto3
from .utilities import utils

stepfunctions = boto3.client('stepfunctions')
sf_arn = utils.get_param('stepfunction_arn')


def lambda_handler(event, _context):
    '''Starts step function execution'''

    sf_input = json.dumps(event, separators=(',', ':'))
    result = stepfunctions.start_execution(
        stateMachineArn=sf_arn,
        input=sf_input
    )
    logging.info('Started step function execution: %s', result['executionArn'])
