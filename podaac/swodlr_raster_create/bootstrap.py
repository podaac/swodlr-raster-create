'''Lambda to bootstrap step function execution'''
import json
import logging
import boto3
from .utils import get_param

stepfunctions = boto3.client('stepfunctions')
sf_arn = get_param('stepfunction_arn')


def lambda_handler(event, _context):
    '''Starts step function execution'''

    sf_input = json.dumps(event, separators=(',', ':'))
    result = stepfunctions.start_execution(arn=sf_arn, input=sf_input)
    logging.info('Started step function execution: %s', result['executionArn'])
