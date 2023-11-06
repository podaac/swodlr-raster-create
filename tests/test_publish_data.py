'''Tests for the publish_data lambda'''
import json
from os import environ
from pathlib import Path
import re
from unittest import TestCase
from unittest.mock import patch

with (
    patch.dict(environ, {
        'SWODLR_ENV': 'dev',
        'SWODLR_publish_bucket': 'publish_bucket'
    }),
    patch('boto3.client')
):
    from podaac.swodlr_raster_create import publish_data


class TestPublishData(TestCase):
    '''Tests for the publish_data lambda'''
    data_path = Path(__file__).parent.joinpath('data')
    failed_jobset_path = data_path.joinpath('failed_jobset.json')
    success_jobset_path = data_path.joinpath('success_jobset.json')
    with failed_jobset_path.open('r', encoding='utf-8') as f:
        failed_jobset = json.load(f)
    with success_jobset_path.open('r', encoding='utf-8') as f:
        success_jobset = json.load(f)

    def tearDown(self):
        publish_data.s3.reset_mock()

    def test_pass_through(self):
        '''
        Test to ensure that a failed state passes through without any calls
        '''
        with patch('otello.mozart.Mozart.get_job_by_id') as mock:
            result = publish_data.lambda_handler(self.failed_jobset, None)

            mock.assert_not_called()
            publish_data.s3.copy.assert_not_called()

        self.assertEqual(self.failed_jobset, result)

    def test_publish(self):
        '''
        Test to ensure that a success state results in a published dataset
        '''
        with patch('otello.mozart.Mozart.get_job_by_id') as mock:
            mock().get_generated_products.return_value = [
                {
                    'dataset': 'test-dataset',
                    'urls': ['s3://hostname:80/sds_bucket/prefix']
                }
            ]
            publish_data.s3.list_objects_v2.return_value = {
                'Contents': [
                    {'Key': 'prefix/test.nc'}
                ]
            }

            result = publish_data.lambda_handler(self.success_jobset, None)

        self.assertEqual(len(result['jobs']), 1)

        granules = result['jobs'][0]['granules']
        self.assertEqual(len(granules), 1)

        granule_uri_match = re.fullmatch(
            r's3://publish_bucket/(test-dataset/24168643-1002-45f5-a059-0b5266bc28f3/\d+/test\.nc)',  # pylint: disable=line-too-long # noqa: E501
            granules[0]
        )
        self.assertIsNotNone(granule_uri_match)

        expected_copy_source = {
            'Bucket': 'sds_bucket',
            'Key': 'prefix/test.nc'
        }
        publish_data.s3.copy.assert_called_once_with(
            CopySource=expected_copy_source,
            Bucket='publish_bucket',
            Key=granule_uri_match.group(1)
        )
