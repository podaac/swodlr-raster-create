'''Performs input granule checks prior to raster product generation'''
from collections import namedtuple
import json
from urllib.parse import urlparse
import requests

from .utilities import utils

STAGE = __name__.rsplit('.', 1)[1]
EDL_TOKEN = utils.get_param('edl_token')
GRAPHQL_ENDPOINT = utils.get_param('cmr_graphql_endpoint')
PIXC_CONCEPT_ID = utils.get_param('pixc_concept_id')
PIXCVEC_CONCEPT_ID = utils.get_param('pixcvec_concept_id')
XDF_ORBIT_CONCEPT_ID = utils.get_param('xdf_orbit_concept_id')

mozart_es_client = utils.get_mozart_es_client()

logger = utils.get_logger(__name__)

validate_input = utils.load_json_schema('input')
validate_jobset = utils.load_json_schema('jobset')

ingest_job_type = utils.mozart_client.get_job_type(
    utils.get_latest_job_version('job-INGEST-STAGED')
)
ingest_job_type.initialize()

Granule = namedtuple('Granule', ('name', 'url'))


def lambda_handler(event, _context):
    '''
    Lambda handler which accepts a SQS message which follows the `input` json
    schema and performs preflight-checks on the requested product. These
    preflight checks ensure that granules required for granule generation
    are ingested into the SDS. This is done by searching against CMR for
    the required granules. Any granules that are in CMR that aren't in GRQ will
    be ingested and any granules that are in GRQ that aren't in CMR will be
    removed to maintain consistency across both systems
    '''
    logger.debug('Records received: %d', len(event['Records']))

    inputs = {}
    jobs = []

    for record in event['Records']:
        body = validate_input(json.loads(record['body']))
        inputs[body['product_id']] = body

        cycle = body['cycle']
        passe = body['pass']
        scene = body['scene']

        cmr_granules = _find_cmr_granules(cycle, passe, scene)
        grq_granules = _find_grq_granules(cycle, passe, scene)

        logger.debug('CMR results: %s', cmr_granules)
        logger.debug('GRQ results: %s', grq_granules)

        cmr_only = cmr_granules - grq_granules
        grq_only = grq_granules - cmr_granules

        ingest_jobs = _ingest_cmr_granules(cmr_only)
        _delete_grq_granules(grq_only)

        for job in ingest_jobs:
            job['product_id'] = body['product_id']
            jobs.append(job)

    jobset = validate_jobset({
        'jobs': jobs,
        'inputs': inputs
    })

    return jobset


def _find_cmr_granules(cycle, passe, scene):
    query = '''
    query($tileParams: GranulesInput, $orbitParams: GranulesInput) {
        tiles: granules(params: $tileParams) {
            items {
                granuleUr
                relatedUrls
            }
        }

        orbit: granules(params: $orbitParams) {
            items {
                granuleUr
                relatedUrls
            }
        }
    }
    '''

    tile_ids = ''.join([
        f'{i:03}L,{i:03}R,' for i in range(scene - 1, scene + 3)
    ])
    variables = {
        'tileParams': {
            'collectionConceptIds': [PIXC_CONCEPT_ID, PIXCVEC_CONCEPT_ID],
            'cycle': str(cycle).rjust(3, '0'),
            'passes': {
                '0': {
                    'pass': str(passe).rjust(3, '0'),
                    'tiles': tile_ids[:-1]
                }
            }
        },

        'orbitParams': {
            'collectionConceptId': XDF_ORBIT_CONCEPT_ID,
            'sortKey': '-end_date',
            'limit': 1
        }
    }

    response = requests.post(
        GRAPHQL_ENDPOINT,
        headers={'Authorization': f'Bearer {EDL_TOKEN}'},
        timeout=15,
        json={
            'query': query,
            'variables': variables
        }
    )

    if not response.ok:
        raise RuntimeError('Experienced network error attempting to reach CMR')

    body = response.json()
    tiles = body['data']['tiles']['items']
    orbit = body['data']['orbit']['items']

    granules = set()
    for granule in tiles + orbit:
        s3_link = _find_s3_link(granule['relatedUrls'])
        if s3_link is None:
            logger.warning('No s3 link found: %s', granule['granuleUr'])
            continue

        granules.add(Granule(
            granule['granuleUr'], _find_s3_link(granule['relatedUrls'])
        ))

    return granules


def _find_grq_granules(cycle, passe, scene):
    collection_ids = ['L2_HR_PIXC', 'L2_HR_PIXCVec']
    tile_ids = [
        str(tile).rjust(3, '0') for tile in range(scene * 2 - 1, scene * 2 + 3)
    ]

    pixc_results = mozart_es_client.search(index='grq', query={
        'bool': {
            'must': [
                {'term': {'dataset_type.keyword': 'SDP'}},
                {'terms': {'dataset.keyword': [collection_ids]}},
                {'term': {'metadata.CycleID': f'{cycle:03}'}},
                {'term': {'metadata.PassID': f'{passe:03}'}},
                {'terms': {'metadata.TileID': tile_ids}}
            ]
        }
    })

    orbit_results = mozart_es_client.search(index='grq', query={
        'bool': {
            'must': [
                {'term': {'dataset_type.keyword': 'AUX'}},
                {'term': {'dataset.keyword': 'XDF_ORBIT_REV_FILE'}}
            ]
        }
    }, sort={'metadata.FileCreationDateTime': {'order': 'desc'}}, size=1)

    logger.debug('PIXC grq results: %s', pixc_results)
    logger.debug('Orbit grq results: %s', orbit_results)

    granules = set()
    for result in pixc_results['hits']['hits'] + orbit_results['hits']['hits']:
        metadata = result['_source']['metadata']
        granules.add(Granule(metadata['id'], metadata['ISL_urls']))

    return granules


def _find_s3_link(related_urls):
    for url in related_urls:
        if not url['type'].startswith('GET DATA'):
            continue

        if urlparse(url['url']).scheme.lower() == 's3':
            return url['url']

        return None


def _ingest_cmr_granules(granules):
    jobs = []

    for granule in granules:
        ingest_job_type.set_input_params(_gen_mozart_job_params(
            granule.name, granule.url
        ))

        job = ingest_job_type.submit_job(
            tag=f'ingest_file_otello__{granule.name}'
        )

        jobs.append({
            'job_id': job.job_id,
            'job_status': 'job-queued',
            'stage': STAGE
        })

    return jobs


def _delete_grq_granules(granules):
    mozart_es_client.delete_by_query(index='grq', query={
        "ids": {"values": [granule.name for granule in granules]}
    })


def _gen_mozart_job_params(filename, url):
    params = {
        'id': filename,
        'data_url': url,
        'data_file': filename,
        'prod_met': {
            'tags': ['ISL', url],
            'met_required': False,
            'restaged': False,
            'ISL_urls': url
        },
        'create_hash': 'false',   # Why is this a string?
        'update_s3_tag': 'false'  # Why is this a string?
    }

    return params
