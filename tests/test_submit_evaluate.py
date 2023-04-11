from collections import namedtuple
import json
import os
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
    patch.dict(os.environ, {
        'SWODLR_ENV': 'dev',
        'SWODLR_sds_username': 'sds_username',
        'SWODLR_sds_password': 'sds_password'
    })
):
    from podaac.swodlr_raster_create import submit_evaluate

    MockJob = namedtuple('MockJob', ['job_id', 'status'])
    submit_evaluate.raster_eval_job_type.submit_job.side_effect = \
        lambda _self: MockJob(
            job_id = str(uuid4()),
            status = 'job-queued'
        )


class TestSubmitEvaluate(TestCase):
    data_path = Path(__file__).parent.joinpath('data')
    valid_sqs_path = data_path.joinpath('valid_sqs.json')
    invalid_sqs_path = data_path.joinpath('invalid_sqs.json')
    with open(valid_sqs_path, encoding='utf-8') as f:
        valid_sqs = json.load(f)
    with open(invalid_sqs_path, encoding='utf-8') as f:
        invalid_sqs = json.load(f)

    def test_valid_submit(self):
        with (
            patch.dict(os.environ, {
                'SWODLR_sds_host': 'http://sds-host.test/',
                'SWODLR_sds_grq_es_path': '/grq_es',
                'SWODLR_sds_grq_es_index': 'grq'
            })
        ):
            results = submit_evaluate.lambda_handler(self.valid_sqs, None)

        # 3 inputted records should = 3 outputted jobs
        self.assertEqual(len(results['jobs']), 3)

        # Assert that stage is properly set on each job
        for job in results['jobs']:
            self.assertEqual(job['stage'], 'submit_evaluate')

        # Check that ES search call performed with proper transformations
        valid_searches = [
            'SWOT_L2_HR_PIXCVec_1_2_4L_*',
            'SWOT_L2_HR_PIXCVec_4_5_10L_*',
            'SWOT_L2_HR_PIXCVec_7_8_16L_*'
        ]
        for call in search_ds_mock.call_args_list:
            search = call.args[0]
            self.assertIn(search, valid_searches)
            valid_searches.remove(search)

        # Check Otello calls performed
        input_dataset_calls = submit_evaluate.raster_eval_job_type \
            .set_input_dataset.call_args_list
        submit_calls = submit_evaluate.raster_eval_job_type.submit_job \
            .call_args_list

        self.assertEqual(len(input_dataset_calls), 3)
        self.assertEqual(len(submit_calls), 3)


    def test_no_pixcvec_error(self):
        dataset_results = {
            'SWOT_L2_HR_PIXCVec_1_2_4L_*': {},
            'SWOT_L2_HR_PIXCVec_4_5_10L_*': None
        }
        search_ds_mock.side_effect = lambda tile: dataset_results.pop(tile)

        with (
            patch.dict(os.environ, {
                'SWODLR_sds_host': 'http://sds-host.test/',
                'SWODLR_sds_grq_es_path': '/grq_es',
                'SWODLR_sds_grq_es_index': 'grq'
            })
        ):
            results = submit_evaluate.lambda_handler(self.invalid_sqs, None)

        # Check that all the search results were returned
        self.assertEqual(len(dataset_results), 0)

        # Check that the returned jobs are properly accepted/rejected
        for job in results['jobs']:
            if job['product_id'] == 'd8fa2f55-2290-42fb-9086-36fbcc5c00d0':
                self.assertEqual(job['job_status'], 'job-failed')
                self.assertEqual(job['errors'], ['Scene does not exist'])
            else:
                self.assertEqual(job['job_status'], 'job-queued')

    def tearDown(self):
        submit_evaluate.raster_eval_job_type.set_input_dataset.reset_mock()
        submit_evaluate.raster_eval_job_type.set_input_params.reset_mock()
        submit_evaluate.raster_eval_job_type.submit_job.reset_mock()
        search_ds_mock.reset_mock(side_effect = True)