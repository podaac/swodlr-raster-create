'''Tests for the wait_for_complete module'''
from copy import deepcopy
import json
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch
from podaac.swodlr_raster_create import wait_for_complete


class TestWaitForComplete(TestCase):
    '''Tests for the wait_for_complete module'''
    data_path = Path(__file__).parent.joinpath('data')
    waiting_jobset_path = data_path.joinpath('waiting_jobset.json')
    with waiting_jobset_path.open('r', encoding='utf-8') as f:
        waiting_jobset = json.load(f)

    def test_skip(self):
        '''
        Tests that the module will update the job status via the SDS and set
        the waiting flag to true when the job is still in-progress while
        passing through all other fields
        '''
        test_status = 'job-started'

        with (
            patch('otello.mozart.Mozart.get_job_by_id') as mock
        ):
            mock().get_info.return_value = {'status': test_status}
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
        '''
        Tests that the module will update the job status via the SDS, set the
        waiting flag to false in the jobset, add the traceback field, and
        passthrough the other fields
        '''

        test_status = 'job-failed'
        test_traceback = \
            '''The place is desert, and let whoso lurks
            In silence, if in this he is aggrieved,
            Break silence now or be forever silent.
            Let him say why it should not be declared so.'''

        with (
            patch('otello.mozart.Mozart.get_job_by_id') as mock
        ):
            mock().get_info.return_value = {
                'status': test_status,
                'traceback': test_traceback
            }
            result = wait_for_complete.lambda_handler(
                deepcopy(self.waiting_jobset), None
            )

        self.assertNotIn('waiting', result)

        result_job = result['jobs'][0]
        waiting_job = self.waiting_jobset['jobs'][0]

        for field in ['product_id', 'stage', 'job_id']:
            self.assertEqual(result_job[field], waiting_job[field])

        self.assertEqual(result_job['job_status'], test_status)
        self.assertEqual(result_job['traceback'], test_traceback)
        self.assertEqual(result_job['errors'], ['SDS threw an error'])
