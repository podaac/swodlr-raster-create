'''Command line tool for submitting an individual granule to the SDS'''
import logging
from argparse import ArgumentParser
import json
import boto3
from time import sleep

logging.basicConfig(level=logging.INFO)
sqs = boto3.client('sqs')


def main():
    '''
    Main entry point for the script
    '''

    parser = ArgumentParser()

    parser.add_argument('queue_url')
    parser.add_argument('job_file')
    parser.add_argument('sleep_duration')

    args = parser.parse_args()
    sleep_duration = int(args.sleep_duration)

    with open(args.job_file, 'r', encoding='utf-8') as f:
        job = json.load(f)

    while True:
        res = sqs.send_message(
            QueueUrl=args.queue_url,
            MessageBody=json.dumps(job)
        )
        logging.info('Sent SQS message; id: %s', res['MessageId'])


        logging.info('Sleeping for %d seconds', sleep_duration)
        sleep(sleep_duration)


if __name__ == '__main__':
    main()
