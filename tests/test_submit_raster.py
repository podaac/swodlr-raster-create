'''Tests for the submit_raster module'''
from collections import namedtuple
import json
from os import environ
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch
from uuid import uuid4

# pylint: disable=duplicate-code
with (
    patch('boto3.client'),
    patch('boto3.resource'),
    patch('otello.mozart.Mozart.get_job_type'),
    # pylint: disable=duplicate-code
    patch.dict(environ, {
        'SWODLR_ENV': 'dev',
        'SWODLR_sds_username': 'sds_username',
        'SWODLR_sds_password': 'sds_password',
        'SWODLR_sds_submit_max_attempts': '1',
        'SWODLR_sds_submit_timeout': '0'
    })
):
    from podaac.swodlr_raster_create import submit_raster

    MockJob = namedtuple('MockJob', ['job_id', 'status'])
    submit_raster.raster_job_type.submit_job.side_effect = \
        lambda tag: MockJob(
            job_id=str(uuid4()),
            status='job-queued'
        )


class TestSubmitRaster(TestCase):
    '''Tests for the submit_raster module'''
    data_path = Path(__file__).parent.joinpath('data')
    failed_jobset_path = data_path.joinpath('failed_jobset.json')
    success_jobset_path = data_path.joinpath('success_jobset.json')
    with failed_jobset_path.open('r', encoding='utf-8') as f:
        failed_jobset = json.load(f)
    with success_jobset_path.open('r', encoding='utf-8') as f:
        success_jobset = json.load(f)

    def test_failed_submit(self):
        '''
        Test that the module passes through failed jobs in a jobset unchanged
        '''
        results = submit_raster.lambda_handler(self.failed_jobset, None)

        # Job should pass through unchanged
        self.assertDictEqual(results, self.failed_jobset)

    def test_success_submit(self):
        '''
        Tests that a valid evaluate job converts to a valid configuration which
        is then utilized to submit a raster job to the SDS with input
        translations and a jobset containing the new job is outputted carrying
        over the previous metadata
        '''
        dummy_dataset_id = 'L2_HR_Raster_001_002_003-state-config'
        dummy_dataset = {'id': dummy_dataset_id}

        def search_dataset_mock(name, _wildcard):
            self.assertEqual(
                name, dummy_dataset_id,
                'Wrong dataset name passed in'
            )
            return dummy_dataset

        with (
            patch.dict(environ, {
                'SWODLR_sds_host': 'http://sds-host.test/',
                'SWODLR_sds_grq_es_path': '/grq_es',
                'SWODLR_sds_grq_es_index': 'grq'
            }),
            patch(
                'podaac.swodlr_raster_create.utilities.Utilities.search_datasets'  # noqa: E501
            ) as search_ds_mock
        ):
            search_ds_mock.side_effect = search_dataset_mock
            results = submit_raster.lambda_handler(self.success_jobset, None)

        input_job = self.success_jobset['jobs'][0]
        self.assertEqual(len(results['jobs']), 1)

        job = results['jobs'][0]
        self.assertEqual(job['stage'], 'submit_raster')
        self.assertEqual(job['job_status'], 'job-queued')
        self.assertEqual(job['product_id'], input_job['product_id'])
        self.assertNotEqual(job['job_id'], input_job['job_id'])

        # Check input params passed along
        self.assertEqual(results['inputs'], self.success_jobset['inputs'])

        # Check Otello calls performed
        # pylint: disable=no-member
        submit_raster.raster_job_type.submit_job.assert_called_once()
        input_dataset_call = submit_raster.raster_job_type.set_input_dataset \
            .call_args_list[0]
        input_params_call = submit_raster.raster_job_type.set_input_params \
            .call_args_list[0]
        # pylint: enable=no-member

        self.assertEqual(input_dataset_call.args[0], dummy_dataset)

        # Check that input transformations performed
        input_params = input_params_call.args[0]
        self.assertEqual(input_params['output_sampling_grid_type'], 'utm')
        self.assertEqual(input_params['output_granule_extent_flag'], 1)

    def tearDown(self):
        # pylint: disable=no-member
        submit_raster.raster_job_type.set_input_dataset.reset_mock()
        submit_raster.raster_job_type.set_input_params.reset_mock()
        submit_raster.raster_job_type.submit_job.reset_mock()
        # pylint: enable=no-member
