from copy import deepcopy
import json
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch
from otello.mozart import Job
from podaac.swodlr_raster_create import wait_for_complete

class TestWaitForComplete(TestCase):
    data_path = Path(__file__).parent.joinpath('data')
    waiting_jobset_path = data_path.joinpath('waiting_jobset.json')
    with open(waiting_jobset_path, 'r') as f:
        waiting_jobset = json.load(f)


    def test_skip(self):
        test_status = 'job-started'
        
        with (
            patch('otello.mozart.Mozart.get_job_by_id') as mock
        ):
            mock().get_info.side_effect = lambda: {'status': test_status}
            result = wait_for_complete.lambda_handler(
                deepcopy(self.waiting_jobset), None
            )

        result_job = result['jobs'][0]
        waiting_job = self.waiting_jobset['jobs'][0]
    
        for field in ['product_id', 'stage', 'job_id']:
            self.assertEqual(result_job[field], waiting_job[field])

        self.assertEqual(result_job['job_status'], test_status)
        self.assertTrue(result['waiting'])

    def test_failed(self):
        test_status = 'job-failed'
        test_traceback = 'It must be I want life to go on living.'

        with (
            patch('otello.mozart.Mozart.get_job_by_id') as mock
        ):
            mock().get_info.side_effect = lambda: {
                'status': test_status,
                'traceback': test_traceback
            }
            result = wait_for_complete.lambda_handler(
                deepcopy(self.waiting_jobset), None
            )
        
        result_job = result['jobs'][0]
        waiting_job = self.waiting_jobset['jobs'][0]
    
        for field in ['product_id', 'stage', 'job_id']:
            self.assertEqual(result_job[field], waiting_job[field])

        self.assertEqual(result_job['job_status'], test_status)
        self.assertEqual(
            result_job['traceback'],
            'It must be I want life to go on living.'
        )
        self.assertEqual(result_job['errors'], ['SDS threw an error'])
