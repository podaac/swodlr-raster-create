import json
import os
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

with (
    patch('boto3.client'),
    patch('boto3.resource'),
    patch('podaac.swodlr_common.utilities.BaseUtilities.get_latest_job_version'),
    patch('otello.mozart.Mozart.get_job_type'),
    patch.dict(os.environ, {
        'SWODLR_ENV': 'dev',
        'SWODLR_sds_username': 'sds_username',
        'SWODLR_sds_password': 'sds_password',
        'SWODLR_sds_submit_max_attempts': '1',
        'SWODLR_sds_submit_timeout': '0'
    })
):
    from podaac.swodlr_raster_create import submit_evaluate

class TestSubmitEvaluate(TestCase):
    '''Tests for the submit_evaluate module'''
    data_path = Path(__file__).parent.joinpath('data')
    valid_sqs_path = data_path.joinpath('valid_sqs.json')
    invalid_sqs_path = data_path.joinpath('invalid_sqs.json')
    with valid_sqs_path.open('r', encoding='utf-8') as f:
        valid_sqs = json.load(f)
    with invalid_sqs_path.open('r', encoding='utf-8') as f:
        invalid_sqs = json.load(f)
        
    def test_valid_submit(self):
        with (
            patch.dict(os.environ, {
                'SWODLR_sds_host': 'http://sds-host.test/',
                'SWODLR_sds_grq_es_path': '/grq_es',
                'SWODLR_sds_grq_es_index': 'grq'
            }),
            patch(
                'podaac.swodlr_raster_create.utilities.utils.search_datasets'
            ) as search_ds_mock
        ):
            results = submit_evaluate.lambda_handler(self.valid_sqs, None)