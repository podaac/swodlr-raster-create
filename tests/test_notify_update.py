'''Tests for the notify_update module'''
import json
from os import environ
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

with (
    patch('boto3.client'),
    patch.dict(environ, {
        'SWODLR_ENV': 'dev',
        'SWODLR_update_topic_arn': 'update_topic_arn',
        'SWODLR_update_max_attempts': '3'
    })
):
    from podaac.swodlr_raster_create import notify_update


class TestQueueUpdate(TestCase):
    '''Tests for the notify_update module'''
    data_path = Path(__file__).parent.joinpath('data')
    success_jobset_path = data_path.joinpath('success_jobset.json')
    with success_jobset_path.open('r', encoding='utf-8') as f:
        success_jobset = json.load(f)

    sns = notify_update.sns

    def test_success(self):
        '''
        Tests that the module calls SNS's publish_batch only once when all the
        messages successfully send on first try
        '''
        self.sns.publish_batch.return_value = {
            'Successful': [{'Id': '24168643-1002-45f5-a059-0b5266bc28f3'}],
            'Failed': []
        }
        notify_update.lambda_handler(self.success_jobset, None)

        self.assertEqual(self.sns.publish_batch.call_count, 1)
        self.assertEqual(
            self.sns.publish_batch.call_args.kwargs['TopicArn'],
            'update_topic_arn'
        )

        entries = self.sns.publish_batch \
            .call_args.kwargs['PublishBatchRequestEntries']
        self.assertEqual(len(entries), 1)

        entry = entries[0]
        job = self.success_jobset['jobs'][0]

        self.assertEqual(entry['Id'], job['product_id'])
        self.assertDictEqual(json.loads(entry['Message']), job)

    def test_retry(self):
        '''
        Tests that the module calls SNS's publish_batch twice to send a message
        when a send fails but it isn't the sender's fault
        '''
        self.sns.publish_batch.side_effect = [
            {
                'Successful': [],
                'Failed': [{
                    'Id': '24168643-1002-45f5-a059-0b5266bc28f3',
                    'Code': 'SWODLR.Test.Error',
                    'SenderFault': False
                }]
            },
            {
                'Successful': [{'Id': '24168643-1002-45f5-a059-0b5266bc28f3'}],
                'Failed': []
            },
        ]

        notify_update.lambda_handler(self.success_jobset, None)

        self.assertEqual(self.sns.publish_batch.call_count, 2)

        job = self.success_jobset['jobs'][0]
        calls = self.sns.publish_batch.call_args_list
        for call in calls:
            self.assertEqual(call.kwargs['TopicArn'], 'update_topic_arn')

            entries = call.kwargs['PublishBatchRequestEntries']
            self.assertEqual(len(entries), 1)

            entry = entries[0]
            self.assertEqual(entry['Id'], job['product_id'])
            self.assertDictEqual(json.loads(entry['Message']), job)

    def test_sender_fail(self):
        '''
        Tests that the module calls SNS's publish_batch only once to send a
        message when a send fails and it's the sender's fault
        '''
        self.sns.publish_batch.return_value = {
            'Successful': [],
            'Failed': [{
                'Id': '24168643-1002-45f5-a059-0b5266bc28f3',
                'Code': 'SWODLR.Test.Error',
                'SenderFault': True
            }]
        }

        notify_update.lambda_handler(self.success_jobset, None)

        self.assertEqual(self.sns.publish_batch.call_count, 1)
        self.assertEqual(
            self.sns.publish_batch.call_args.kwargs['TopicArn'],
            'update_topic_arn'
        )

        entries = self.sns.publish_batch \
            .call_args.kwargs['PublishBatchRequestEntries']
        self.assertEqual(len(entries), 1)

        entry = entries[0]
        job = self.success_jobset['jobs'][0]
        self.assertEqual(entry['Id'], job['product_id'])
        self.assertDictEqual(json.loads(entry['Message']), job)

    def test_complete_fail(self):
        '''
        Tests that the module calls SNS's publish_batch up to the max attempt
        value when every call results in a failed publish and it's not the
        sender's fault
        '''
        self.sns.publish_batch.return_value = {
            'Successful': [],
            'Failed': [{
                'Id': '24168643-1002-45f5-a059-0b5266bc28f3',
                'Code': 'SWODLR.Test.Error',
                'SenderFault': False
            }]
        }

        with self.assertRaises(RuntimeError):
            notify_update.lambda_handler(self.success_jobset, None)

        self.assertEqual(self.sns.publish_batch.call_count, 3)

        job = self.success_jobset['jobs'][0]
        calls = self.sns.publish_batch.call_args_list
        for call in calls:
            self.assertEqual(call.kwargs['TopicArn'], 'update_topic_arn')

            entries = call.kwargs['PublishBatchRequestEntries']
            self.assertEqual(len(entries), 1)

            entry = entries[0]
            self.assertEqual(entry['Id'], job['product_id'])
            self.assertDictEqual(json.loads(entry['Message']), job)

    def tearDown(self):
        self.sns.reset_mock(side_effect=True)
