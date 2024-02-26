'''Tests for the preflight module'''
from collections import namedtuple
import json
import os
from pathlib import Path
from requests import Response
from unittest import TestCase
from unittest.mock import Mock, patch
from uuid import uuid4


# pylint: disable=duplicate-code
with (
    patch.dict(os.environ, {
        'SWODLR_ENV': 'dev',
        'SWODLR_cmr_graphql_endpoint': 'http://cmr-graphql.test/',
        'SWODLR_sds_host': 'http://sds-host.test/',
        'SWODLR_sds_username': 'sds_username',
        'SWODLR_sds_password': 'sds_password',
        'SWODLR_sds_submit_max_attempts': '1',
        'SWODLR_sds_submit_timeout': '0'
    }),
    patch('boto3.client'),
    patch('boto3.resource'),
    patch('otello.mozart.Mozart.get_job_type'),
    patch('podaac.swodlr_common.utilities.BaseUtilities.get_latest_job_version'),
    patch('podaac.swodlr_raster_create.utilities.utils.get_mozart_es_client') as mock_es_client
):
    from podaac.swodlr_raster_create import preflight

    MockJob = namedtuple('MockJob', ['job_id', 'status'])
    preflight.ingest_job_type.submit_job.side_effect = \
        lambda tag: MockJob(
            job_id=str(uuid4()),
            status='job-queued'
        )


class TestPreflight(TestCase):
    '''Tests for the preflight module'''
    data_path = Path(__file__).parent.joinpath('data')
    valid_sqs_path = data_path.joinpath('valid_sqs.json')
    invalid_sqs_path = data_path.joinpath('invalid_sqs.json')
    with valid_sqs_path.open('r', encoding='utf-8') as f:
        valid_sqs = json.load(f)
    with invalid_sqs_path.open('r', encoding='utf-8') as f:
        invalid_sqs = json.load(f)

    def test_no_action(self):
        with (
            patch('requests.post') as mock_post
        ):
            # -- CMR Mock --
            mock_cmr_response = Mock(spec=Response)
            mock_cmr_response.status_code = 200
            mock_cmr_response.json.return_value = {
                'data': {
                    'tiles': {
                        'items': [{
                            'granuleUr': 'SWODLR_TEST_GRANULE_1',
                            'relatedUrls': [{
                                'type': 'GET DATA',
                                'url': 's3://dummy-bucket/test_1.nc'
                            }]
                        }, {
                            'granuleUr': 'SWODLR_TEST_GRANULE_2',
                            'relatedUrls': [{
                                'type': 'GET DATA',
                                'url': 's3://dummy-bucket/test_2.nc'
                            }]
                        }, {
                            'granuleUr': 'SWODLR_TEST_GRANULE_3',
                            'relatedUrls': [{
                                'type': 'GET DATA',
                                'url': 's3://dummy-bucket/test_3.nc'
                            }]
                        }, {
                            'granuleUr': 'SWODLR_TEST_GRANULE_4',
                            'type': 'GET DATA',
                            'relatedUrls': [{
                                'type': 'GET DATA',
                                'url': 's3://dummy-bucket/test_4.nc'
                            }]
                        }]
                    },
                    'orbit': {
                        'items': [{
                            'granuleUr': 'SWODLR_TEST_GRANULE_5',
                            'relatedUrls': [{
                                'type': 'GET DATA',
                                'url': 's3://dummy-bucket/test_5.nc'
                            }]
                        }]
                    }
                }
            }
            
            mock_post.return_value = mock_cmr_response
            
            # -- GRQ Mock --
            mock_grq_tile_response = {
                'hits': {
                    'hits': [{
                        '_source': {
                            'metadata': {
                                'id': 'SWODLR_TEST_GRANULE_1',
                                'ISL_urls': 's3://dummy-bucket/test_1.nc'
                            }
                        }
                    }, {
                        '_source': {
                            'metadata': {
                                'id': 'SWODLR_TEST_GRANULE_2',
                                'ISL_urls': 's3://dummy-bucket/test_2.nc'
                            }
                        }
                    }, {
                        '_source': {
                            'metadata': {
                                'id': 'SWODLR_TEST_GRANULE_3',
                                'ISL_urls': 's3://dummy-bucket/test_3.nc'
                            }
                        }
                    }, {
                        '_source': {
                            'metadata': {
                                'id': 'SWODLR_TEST_GRANULE_4',
                                'ISL_urls': 's3://dummy-bucket/test_4.nc'
                            }
                        }
                    }]
                }
            }
            
            mock_grq_orbit_response = {
                'hits': {
                    'hits': [{
                        '_source': {
                            'metadata': {
                                'id': 'SWODLR_TEST_GRANULE_5',
                                'ISL_urls': 's3://dummy-bucket/test_5.nc'
                            }
                        }
                    }]
                }
            }
            
            mock_es_client.search.side_effect = (mock_grq_tile_response, mock_grq_orbit_response)

            # Lambda handler call
            results = preflight.lambda_handler(self.valid_sqs, None)

            # Assertion checks
            post_calls = mock_post.call_args_list
            self.assertEqual(len(post_calls), 1)
            self.assertTupleEqual(post_calls[0].args, ('http://cmr-graphql.test/',))
            
            
            mock_es_client().search.assert_called()
 
    
    def test_clear_sds(self):
        pass
    
    def test_ingest_new(self):
        pass

    def tearDown(self):
        # pylint: disable=no-member
        preflight.ingest_job_type.set_input_params.reset_mock()
        preflight.ingest_job_type.submit_job.reset_mock()
        # pylint: enable=no-member
