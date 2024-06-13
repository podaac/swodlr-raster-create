'''Tests the submit_evaluate module'''
import json
import os
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock, patch

with (
    patch('boto3.client'),
    patch('boto3.resource'),
    patch('podaac.swodlr_common.utilities.BaseUtilities.get_latest_job_version'),  # noqa: E501
    patch('otello.mozart.Mozart.get_job_type'),
    patch('podaac.swodlr_raster_create.utilities.utils.get_grq_es_client') as mock_es_client,  # pylint: disable=line-too-long # noqa: E501
    patch.dict(os.environ, {
        'SWODLR_ENV': 'dev',
        'SWODLR_sds_username': 'sds_username',
        'SWODLR_sds_password': 'sds_password',
        'SWODLR_sds_submit_max_attempts': '1',
        'SWODLR_sds_submit_timeout': '0',
        'SWODLR_sds_host': 'http://sds-host.test/',
        'SWODLR_sds_grq_es_path': '/grq_es',
        'SWODLR_sds_grq_es_index': 'grq'
    })
):
    from podaac.swodlr_raster_create import submit_evaluate


class TestSubmitEvaluate(TestCase):
    '''Tests for the submit_evaluate module'''
    data_path = Path(__file__).parent.joinpath('data')
    success_jobset_path = data_path.joinpath('success_jobset.json')
    with success_jobset_path.open('r', encoding='utf-8') as f:
        success_jobset = json.load(f)

    def test_successful_submit(self):
        '''
        Test to check that the submit_evaluate module will submit a job to the
        SDS given that the initial scene granule can be located
        '''

        # Setup mocks
        mock_es_client().search.return_value = {'hits': {'hits': [
            MagicMock()]}}
        submit_evaluate.raster_eval_job_type.submit_job.return_value = \
            MagicMock(job_id='72c4b5a0-f772-4311-b78d-d0d947b5db11')

        # Lambda call
        results = submit_evaluate.lambda_handler(self.success_jobset, None)

        # Assertion checks
        mock_es_client().search.assert_called_once_with(
            # pylint: disable=duplicate-code
            index='grq',
            size=10,
            body={
                'query': {
                    'bool': {
                        'must': [
                            {'term': {'dataset_type.keyword': 'SDP'}},
                            {'term': {'dataset.keyword': 'L2_HR_PIXC'}},
                            {'term': {'metadata.CycleID': '001'}},
                            {'term': {'metadata.PassID': '002'}},
                            {'terms': {'metadata.TileID': ['005', '006']}}
                        ]
                    }
                }
            }
        )
        submit_evaluate.raster_eval_job_type.submit_job.assert_called_once()  # pylint: disable=no-member # noqa: E501
        # Results check
        self.assertDictEqual(results, {
            'jobs': [{
                'stage': 'submit_evaluate',
                'product_id': '24168643-1002-45f5-a059-0b5266bc28f3',
                'job_id': '72c4b5a0-f772-4311-b78d-d0d947b5db11',
                'job_status': 'job-queued'
            }],
            'inputs': {
                '24168643-1002-45f5-a059-0b5266bc28f3': {
                    'product_id': '24168643-1002-45f5-a059-0b5266bc28f3',
                    'cycle': 1,
                    'pass': 2,
                    'scene': 3,
                    'raster_resolution': 100,
                    'output_sampling_grid_type': 'UTM',
                    'output_granule_extent_flag': True,
                    'utm_zone_adjust': 0,
                    'mgrs_band_adjust': 0
                }
            }
        })

    def test_not_found_submit(self):
        '''
        Test to check that the submit_evaluate module will not submit a job to
        the SDS given that the initial scene granule cannot be located; this
        process should fail gracefully (eg no raised exceptions)
        '''

        # Setup mocks
        mock_es_client().search.return_value = {'hits': {'hits': []}}

        # Lambda call
        results = submit_evaluate.lambda_handler(self.success_jobset, None)

        # Assertion checks
        mock_es_client().search.assert_called_once_with(
            # pylint: disable=duplicate-code
            index='grq',
            size=10,
            body={
                'query': {
                    'bool': {
                        'must': [
                            {'term': {'dataset_type.keyword': 'SDP'}},
                            {'term': {'dataset.keyword': 'L2_HR_PIXC'}},
                            {'term': {'metadata.CycleID': '001'}},
                            {'term': {'metadata.PassID': '002'}},
                            {'terms': {'metadata.TileID': ['005', '006']}}
                        ]
                    }
                }
            }
        )
        submit_evaluate.raster_eval_job_type.submit_job.assert_not_called()  # pylint: disable=no-member # noqa: E501

        # Results check
        self.assertDictEqual(results, {
            'jobs': [{
                'stage': 'submit_evaluate',
                'product_id': '24168643-1002-45f5-a059-0b5266bc28f3',
                'job_status': 'job-failed',
                'errors': ['Scene does not exist']
            }],
            'inputs': {
                '24168643-1002-45f5-a059-0b5266bc28f3': {
                    'product_id': '24168643-1002-45f5-a059-0b5266bc28f3',
                    'cycle': 1,
                    'pass': 2,
                    'scene': 3,
                    'raster_resolution': 100,
                    'output_sampling_grid_type': 'UTM',
                    'output_granule_extent_flag': True,
                    'utm_zone_adjust': 0,
                    'mgrs_band_adjust': 0
                }
            }
        })

    def tearDown(self):
        # pylint: disable-next=no-member
        submit_evaluate.raster_eval_job_type.reset_mock()
        mock_es_client.reset_mock()
