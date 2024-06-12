'''Tests for the preflight module'''
from collections import namedtuple
import json
import os
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch
from uuid import uuid4
from requests import Response


# pylint: disable=duplicate-code
with (
    patch.dict(os.environ, {
        'SWODLR_ENV': 'dev',
        'SWODLR_edl_token': 'edl-test-token',
        'SWODLR_pixc_concept_id': 'test-pixc-concept-id',
        'SWODLR_pixcvec_concept_id': 'test-pixcvec-concept-id',
        'SWODLR_xdf_orbit_1.0_concept_id': 'test-xdf-orbit-1.0-concept-id',
        'SWODLR_xdf_orbit_2.0_concept_id': 'test-xdf-orbit-2.0-concept-id',
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
    patch('podaac.swodlr_common.utilities.BaseUtilities.get_latest_job_version'),  # pylint: disable-next=line-too-long # noqa: E501
    patch('podaac.swodlr_raster_create.utilities.utils.get_grq_es_client') as mock_es_client  # pylint: disable-next=line-too-long # noqa: E501
):
    from podaac.swodlr_raster_create import preflight

    def _mock_submit_job(*_args, **_kwargs):
        return MockJob(
            job_id=str(uuid4()),
            status='job-queued'
        )

    MockJob = namedtuple('MockJob', ['job_id', 'status'])
    preflight.ingest_job_type.submit_job.side_effect = _mock_submit_job


class TestPreflight(TestCase):
    '''Tests for the preflight module'''
    data_path = Path(__file__).parent.joinpath('data')
    valid_sqs_path = data_path.joinpath('valid_sqs.json')
    invalid_sqs_path = data_path.joinpath('invalid_sqs.json')
    with valid_sqs_path.open('r', encoding='utf-8') as f:
        valid_sqs = json.load(f)

    def test_no_action(self):
        '''
        Test the situation where GRQ and CMR are at the same state; there
        should be no action taken by preflight
        '''
        with patch('requests.post') as mock_post:
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

            mock_es_client().search.side_effect = (mock_grq_tile_response, mock_grq_orbit_response)  # pylint: disable=line-too-long # noqa: E501

            # Lambda handler call
            results = preflight.lambda_handler(self.valid_sqs, None)

            # Assertion checks
            post_calls = mock_post.call_args_list
            self.assertEqual(len(post_calls), 1)
            self.assertTupleEqual(post_calls[0].args, ('http://cmr-graphql.test/',))  # noqa: E501
            self.assertDictEqual(post_calls[0].kwargs, {
                'headers': {'Authorization': 'Bearer edl-test-token'},
                'timeout': 15,
                'json': {
                    # pylint: disable-next=line-too-long
                    'query': '\n    query($tileParams: GranulesInput, $orbitParams: GranulesInput) {\n        tiles: granules(params: $tileParams) {\n            items {\n                granuleUr\n                relatedUrls\n            }\n        }\n\n        orbit: granules(params: $orbitParams) {\n            items {\n                granuleUr\n                relatedUrls\n            }\n        }\n    }\n    ',  # noqa: E501
                    'variables': {
                        'tileParams': {
                            'collectionConceptIds': [
                                'test-pixc-concept-id',
                                'test-pixcvec-concept-id'
                            ],
                            'cycle': 1,
                            'passes': {
                                '0': {
                                    'pass': 2,
                                    'tiles': '5L,5R,005L,005R,6L,6R,006L,006R,7L,7R,007L,007R,8L,8R,008L,008R'  # pylint: disable=line-too-long # noqa: E501
                                }
                            },
                            'limit': 100
                        },
                        'orbitParams': {
                            'collectionConceptId': 'test-xdf-orbit-2.0-concept-id',  # pylint: disable=line-too-long # noqa: E501
                            'sortKey': '-end_date',
                            'limit': 1
                        }
                    }
                }
            })

            es_search_calls = mock_es_client().search.call_args_list
            self.assertEqual(len(es_search_calls), 2)
            self.assertDictEqual(es_search_calls[0].kwargs, {
                'index': 'grq',
                'body': {
                    'query': {
                        'bool': {
                            'must': [
                                {'term': {'dataset_type.keyword': 'SDP'}},
                                {'terms': {'dataset.keyword': ['L2_HR_PIXC', 'L2_HR_PIXCVec']}},  # pylint: disable=line-too-long # noqa: E501
                                {'term': {'metadata.CycleID': '001'}},
                                {'term': {'metadata.PassID': '002'}},
                                {'terms': {'metadata.TileID': ['005', '006', '007', '008']}}  # pylint: disable=line-too-long # noqa: E501
                            ]
                        }
                    }
                },
                'size': 100
            })
            self.assertDictEqual(es_search_calls[1].kwargs, {
                'index': 'grq',
                'body': {
                    'query': {
                        'bool': {
                            'must': [
                                {'term': {'dataset_type.keyword': 'AUX'}},
                                {'term': {'dataset.keyword': 'XDF_ORBIT_REV_FILE'}}  # noqa: E501
                            ]
                        }
                    },
                    'sort': {'endtime': {'order': 'desc'}}
                },
                'size': 1
            })

            # Results check
            self.assertDictEqual(results, {
                'jobs': [],
                'inputs': {
                    'bd18530a-0383-44ec-8cec-4019892afc2e': {
                        'product_id': 'bd18530a-0383-44ec-8cec-4019892afc2e',
                        'cycle': 1,
                        'pass': 2,
                        'scene': 3,
                        'output_granule_extent_flag': True,
                        'output_sampling_grid_type': 'UTM',
                        'raster_resolution': 100,
                        'utm_zone_adjust': 0,
                        'mgrs_band_adjust': 0
                    }
                }
            })

    def test_clear_sds(self):
        '''
        Test the situation where GRQ is showing granules which are not found
        on a CMR search; preflight should reconcile the difference by deleting
        the entries on GRQ to ensure consistency
        '''

        with patch('requests.post') as mock_post:
            # -- CMR Mock --
            mock_cmr_response = Mock(spec=Response)
            mock_cmr_response.status_code = 200
            mock_cmr_response.json.return_value = {
                'data': {
                    'tiles': {
                        'items': []
                    },
                    'orbit': {
                        'items': []
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

            mock_es_client().search.side_effect = [
                mock_grq_tile_response, mock_grq_orbit_response
            ]

            # Lambda handler call
            results = preflight.lambda_handler(self.valid_sqs, None)

            # Assertion checks
            post_calls = mock_post.call_args_list
            self.assertEqual(len(post_calls), 1)
            self.assertTupleEqual(post_calls[0].args, ('http://cmr-graphql.test/',))  # noqa: E501
            self.assertDictEqual(post_calls[0].kwargs, {
                'headers': {'Authorization': 'Bearer edl-test-token'},
                'timeout': 15,
                'json': {
                    # pylint: disable-next=line-too-long
                    'query': '\n    query($tileParams: GranulesInput, $orbitParams: GranulesInput) {\n        tiles: granules(params: $tileParams) {\n            items {\n                granuleUr\n                relatedUrls\n            }\n        }\n\n        orbit: granules(params: $orbitParams) {\n            items {\n                granuleUr\n                relatedUrls\n            }\n        }\n    }\n    ',  # noqa: E501
                    'variables': {
                        'tileParams': {
                            'collectionConceptIds': [
                                'test-pixc-concept-id',
                                'test-pixcvec-concept-id'
                            ],
                            'cycle': 1,
                            'passes': {
                                '0': {
                                    'pass': 2,
                                    'tiles': '5L,5R,005L,005R,6L,6R,006L,006R,7L,7R,007L,007R,8L,8R,008L,008R'  # pylint: disable=line-too-long # noqa: E501
                                }
                            },
                            'limit': 100
                        },
                        'orbitParams': {
                            'collectionConceptId': 'test-xdf-orbit-2.0-concept-id',  # pylint: disable=line-too-long # noqa: E501
                            'sortKey': '-end_date',
                            'limit': 1
                        }
                    }
                }
            })

            es_search_calls = mock_es_client().search.call_args_list
            self.assertEqual(len(es_search_calls), 2)
            self.assertDictEqual(es_search_calls[0].kwargs, {
                'index': 'grq',
                'body': {
                    'query': {
                        'bool': {
                            'must': [
                                {'term': {'dataset_type.keyword': 'SDP'}},
                                {'terms': {'dataset.keyword': ['L2_HR_PIXC', 'L2_HR_PIXCVec']}},  # pylint: disable=line-too-long # noqa: E501
                                {'term': {'metadata.CycleID': '001'}},
                                {'term': {'metadata.PassID': '002'}},
                                {'terms': {'metadata.TileID': ['005', '006', '007', '008']}}  # pylint: disable=line-too-long # noqa: E501
                            ]
                        }
                    }
                },
                'size': 100
            })

            self.assertDictEqual(es_search_calls[1].kwargs, {
                'index': 'grq',
                'body': {
                    'query': {
                        'bool': {
                            'must': [
                                {'term': {'dataset_type.keyword': 'AUX'}},
                                {'term': {'dataset.keyword': 'XDF_ORBIT_REV_FILE'}}  # noqa: E501
                            ]
                        }
                    },
                    'sort': {'endtime': {'order': 'desc'}}
                },
                'size': 1
            })

            delete_calls = mock_es_client().delete_by_query.call_args_list
            self.assertEqual(len(delete_calls), 1)
            self.assertEqual(delete_calls[0].kwargs['index'], 'grq')
            self.assertCountEqual(
                delete_calls[0].kwargs['body']['query']['ids']['values'],
                [
                    'SWODLR_TEST_GRANULE_1',
                    'SWODLR_TEST_GRANULE_2',
                    'SWODLR_TEST_GRANULE_3',
                    'SWODLR_TEST_GRANULE_4'
                ]
            )

            # Results check
            self.assertDictEqual(results, {
                'jobs': [],
                'inputs': {
                    'bd18530a-0383-44ec-8cec-4019892afc2e': {
                        'product_id': 'bd18530a-0383-44ec-8cec-4019892afc2e',
                        'cycle': 1,
                        'pass': 2,
                        'scene': 3,
                        'output_granule_extent_flag': True,
                        'output_sampling_grid_type': 'UTM',
                        'raster_resolution': 100,
                        'utm_zone_adjust': 0,
                        'mgrs_band_adjust': 0
                    }
                }
            })

    def test_ingest_new(self):
        '''
        Test the situation where CMR is showing granules which are not found
        on GRQ; preflight should reconcile the difference by beginning new
        ingest jobs against the new granules from CMR
        '''

        with patch('requests.post') as mock_post:
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
            mock_grq_tile_response = {'hits': {'hits': []}}
            mock_grq_orbit_response = {'hits': {'hits': []}}

            mock_es_client().search.side_effect = [
                mock_grq_tile_response, mock_grq_orbit_response
            ]

            # Lambda handler call
            results = preflight.lambda_handler(self.valid_sqs, None)

            # Assertion checks
            post_calls = mock_post.call_args_list
            self.assertEqual(len(post_calls), 1)
            self.assertTupleEqual(post_calls[0].args, ('http://cmr-graphql.test/',))  # noqa: E501
            self.assertDictEqual(post_calls[0].kwargs, {
                'headers': {'Authorization': 'Bearer edl-test-token'},
                'timeout': 15,
                'json': {
                    # pylint: disable-next=line-too-long
                    'query': '\n    query($tileParams: GranulesInput, $orbitParams: GranulesInput) {\n        tiles: granules(params: $tileParams) {\n            items {\n                granuleUr\n                relatedUrls\n            }\n        }\n\n        orbit: granules(params: $orbitParams) {\n            items {\n                granuleUr\n                relatedUrls\n            }\n        }\n    }\n    ',  # noqa: E501
                    'variables': {
                        'tileParams': {
                            'collectionConceptIds': [
                                'test-pixc-concept-id',
                                'test-pixcvec-concept-id'
                            ],
                            'cycle': 1,
                            'passes': {
                                '0': {
                                    'pass': 2,
                                    'tiles': '5L,5R,005L,005R,6L,6R,006L,006R,7L,7R,007L,007R,8L,8R,008L,008R'  # pylint: disable=line-too-long # noqa: E501
                                }
                            },
                            'limit': 100
                        },
                        'orbitParams': {
                            'collectionConceptId': 'test-xdf-orbit-2.0-concept-id',  # pylint: disable=line-too-long # noqa: E501
                            'sortKey': '-end_date',
                            'limit': 1
                        }
                    }
                }
            })

            es_search_calls = mock_es_client().search.call_args_list
            self.assertEqual(len(es_search_calls), 2)
            self.assertDictEqual(es_search_calls[0].kwargs, {
                'index': 'grq',
                'body': {
                    'query': {
                        'bool': {
                            'must': [
                                {'term': {'dataset_type.keyword': 'SDP'}},
                                {'terms': {'dataset.keyword': ['L2_HR_PIXC', 'L2_HR_PIXCVec']}},  # pylint: disable=line-too-long # noqa: E501
                                {'term': {'metadata.CycleID': '001'}},
                                {'term': {'metadata.PassID': '002'}},
                                {'terms': {'metadata.TileID': ['005', '006', '007', '008']}}  # pylint: disable=line-too-long # noqa: E501
                            ]
                        }
                    }
                },
                'size': 100
            })
            self.assertDictEqual(es_search_calls[1].kwargs, {
                'index': 'grq',
                'body': {
                    'query': {
                        'bool': {
                            'must': [
                                {'term': {'dataset_type.keyword': 'AUX'}},
                                {'term': {'dataset.keyword': 'XDF_ORBIT_REV_FILE'}}  # noqa: E501
                            ]
                        }
                    },
                    'sort': {'endtime': {'order': 'desc'}}
                },
                'size': 1
            })

            # pylint: disable-next=no-member
            self.assertEqual(preflight.ingest_job_type.submit_job.call_count, 5)  # noqa: E501

            # Results check
            self.assertDictEqual(results['inputs'], {
                    'bd18530a-0383-44ec-8cec-4019892afc2e': {
                        'product_id': 'bd18530a-0383-44ec-8cec-4019892afc2e',
                        'cycle': 1,
                        'pass': 2,
                        'scene': 3,
                        'output_granule_extent_flag': True,
                        'output_sampling_grid_type': 'UTM',
                        'raster_resolution': 100,
                        'utm_zone_adjust': 0,
                        'mgrs_band_adjust': 0
                    }
            })
            self.assertEqual(len(results['jobs']), 5)

            for job in results['jobs']:
                self.assertEqual(job['job_status'], 'job-queued')
                self.assertEqual(job['product_id'], 'bd18530a-0383-44ec-8cec-4019892afc2e')  # pylint: disable=line-too-long # noqa: E501
                self.assertEqual(job['stage'], 'preflight')

    def tearDown(self):
        # pylint: disable=no-member
        preflight.ingest_job_type.set_input_params.reset_mock()
        preflight.ingest_job_type.submit_job.reset_mock()
        mock_es_client.reset_mock()
        # pylint: enable=no-member
