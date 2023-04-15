import json
from os import environ
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

with (
    patch('boto3.resource'),
    patch.dict(environ, {
        'SWODLR_ENV': 'dev',
        'SWODLR_update_queue_url': 'queue_url',
        'SWODLR_update_queue_max_attempts': '2'
    })
):
    from podaac.swodlr_raster_create import queue_update
    from podaac.swodlr_raster_create.utils import update_queue


class TestQueueUpdate(TestCase):
    data_path = Path(__file__).parent.joinpath('data')
    success_jobset_path = data_path.joinpath('success_jobset.json')
    with success_jobset_path.open('r') as f:
        success_jobset = json.load(f)

    def test_success(self):
        update_queue.send_messages.return_value = {
            'Successful': [{'Id': '24168643-1002-45f5-a059-0b5266bc28f3'}],
            'Failed': []
        }
        queue_update.lambda_handler(self.success_jobset, None)

        self.assertEqual(update_queue.send_messages.call_count, 1)
        entries = update_queue.send_messages.call_args.kwargs['Entries']
        self.assertEqual(len(entries), 1)

        entry = entries[0]
        job = self.success_jobset['jobs'][0]

        self.assertEqual(entry['Id'], job['product_id'])
        self.assertDictEqual(json.loads(entry['MessageBody']), job)

    def test_retry(self):
        update_queue.send_messages.side_effect = [
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

        queue_update.lambda_handler(self.success_jobset, None)

        self.assertEqual(update_queue.send_messages.call_count, 2)

        job = self.success_jobset['jobs'][0]
        calls = update_queue.send_messages.call_args_list
        for call in calls:
            entries = call.kwargs['Entries']
            self.assertEqual(len(entries), 1)

            entry = entries[0]
            self.assertEqual(entry['Id'], job['product_id'])
            self.assertDictEqual(json.loads(entry['MessageBody']), job)

    def test_sender_fail(self):
        update_queue.send_messages.return_value = {
            'Successful': [],
            'Failed': [{
                'Id': '24168643-1002-45f5-a059-0b5266bc28f3',
                'Code': 'SWODLR.Test.Error',
                'SenderFault': True
            }]
        }

        queue_update.lambda_handler(self.success_jobset, None)

        self.assertEqual(update_queue.send_messages.call_count, 1)
        entries = update_queue.send_messages.call_args.kwargs['Entries']
        self.assertEqual(len(entries), 1)

        entry = entries[0]
        job = self.success_jobset['jobs'][0]
        self.assertEqual(entry['Id'], job['product_id'])
        self.assertDictEqual(json.loads(entry['MessageBody']), job)

    def test_complete_fail(self):
        update_queue.send_messages.return_value = {
            'Successful': [],
            'Failed': [{
                'Id': '24168643-1002-45f5-a059-0b5266bc28f3',
                'Code': 'SWODLR.Test.Error',
                'SenderFault': False
            }]
        }

        with self.assertRaises(RuntimeError):
            queue_update.lambda_handler(self.success_jobset, None)

        self.assertEqual(update_queue.send_messages.call_count, 2)

        job = self.success_jobset['jobs'][0]
        calls = update_queue.send_messages.call_args_list
        for call in calls:
            entries = call.kwargs['Entries']
            self.assertEqual(len(entries), 1)

            entry = entries[0]
            self.assertEqual(entry['Id'], job['product_id'])
            self.assertDictEqual(json.loads(entry['MessageBody']), job)

    def tearDown(self):
        update_queue.reset_mock(side_effect=True)
