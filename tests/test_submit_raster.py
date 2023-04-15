from collections import namedtuple
import json
from os import environ
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch
from uuid import uuid4


with (
    patch('boto3.client'),
    patch('boto3.resource'),
    patch(
        'podaac.swodlr_raster_create.utils.search_datasets'
    ) as search_ds_mock,
    patch('otello.mozart.Mozart.get_job_type'),
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
            job_id = str(uuid4()),
            status = 'job-queued'
        )

class TestSubmitRaster(TestCase):
    data_path = Path(__file__).parent.joinpath('data')
    failed_jobset_path = data_path.joinpath('failed_jobset.json')
    success_jobset_path = data_path.joinpath('success_jobset.json')
    with failed_jobset_path.open('r') as f:
        failed_jobset = json.load(f)
    with success_jobset_path.open('r') as f:
        success_jobset = json.load(f)

    def test_failed_submit(self):
        results = submit_raster.lambda_handler(self.failed_jobset, None)

        # Job should pass through unchanged
        self.assertDictEqual(results, self.failed_jobset)

    def test_success_submit(self):
        dummy_dataset_id = 'DUMMY_DATASET'
        dummy_dataset = {'id': dummy_dataset_id}
        def search_dataset_dummy(name, _wildcard):
            self.assertEqual(
                name, dummy_dataset_id,
                'Wrong dataset name passed in'
            )
            return dummy_dataset
    
        search_ds_mock.side_effect = search_dataset_dummy

        with (
            patch('otello.mozart.Mozart.get_job_by_id') as get_job_by_id_mock,
            patch.dict(environ, {
                'SWODLR_sds_host': 'http://sds-host.test/',
                'SWODLR_sds_grq_es_path': '/grq_es',
                'SWODLR_sds_grq_es_index': 'grq'
            })
        ):
            get_job_by_id_mock().get_generated_products.side_effect = \
                lambda: [{'id': dummy_dataset_id}]
            results = submit_raster.lambda_handler(self.success_jobset, None)

        input_job = self.success_jobset['jobs'][0]
        self.assertEqual(len(results['jobs']), 1)

        job = results['jobs'][0]
        self.assertEqual(job['stage'], 'submit_raster')
        self.assertEqual(job['job_status'], 'job-queued')
        self.assertEqual(job['product_id'], input_job['product_id'])
        self.assertNotEqual(job['job_id'], input_job['job_id'])

        # Check metadata passed along
        self.assertEqual(job['metadata'], input_job['metadata'])

        # Check Otello calls performed
        submit_raster.raster_job_type.submit_job.assert_called_once()
        input_dataset_call = submit_raster.raster_job_type.set_input_dataset \
            .call_args_list[0]
        input_params_call = submit_raster.raster_job_type.set_input_params \
            .call_args_list[0]
        
        self.assertEqual(input_dataset_call.args[0], dummy_dataset)
        
        # Check that input transformations performed
        input_params = input_params_call.args[0]
        self.assertEqual(input_params['output_sampling_grid_type'], 'utm')
        self.assertEqual(input_params['output_granule_extent_flag'], 1)

        # Check that other params are passed directly through
        param_names = [
            'raster_resolution',
            'utm_zone_adjust',
            'mgrs_band_adjust'
        ]
        input_job_metadata = input_job['metadata']
        for name in param_names:
            self.assertEqual(input_params[name], input_job_metadata[name])


    def tearDown(self):
        submit_raster.raster_job_type.set_input_dataset.reset_mock()
        submit_raster.raster_job_type.set_input_params.reset_mock()
        submit_raster.raster_job_type.submit_job.reset_mock()
        search_ds_mock.reset_mock(side_effect=True)
