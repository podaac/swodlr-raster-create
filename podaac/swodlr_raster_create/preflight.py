'''Performs input granule checks prior to raster product generation'''
from collections import namedtuple
import json
from pathlib import PurePath
from urllib.parse import urlparse
import requests

from .utilities import utils

STAGE = __name__.rsplit('.', 1)[1]
EDL_TOKEN = utils.get_param('edl_token')
GRAPHQL_ENDPOINT = utils.get_param('cmr_graphql_endpoint')
PIXC_CONCEPT_ID = utils.get_param('pixc_concept_id')
PIXCVEC_CONCEPT_ID = utils.get_param('pixcvec_concept_id')
XDF_ORBIT_CONCEPT_ID = utils.get_param('xdf_orbit_concept_id')

grq_es_client = utils.get_grq_es_client()

logger = utils.get_logger(__name__)

validate_input = utils.load_json_schema('input')
validate_jobset = utils.load_json_schema('jobset')

ingest_job_type = utils.mozart_client.get_job_type(
    utils.get_latest_job_version('job-INGEST_STAGED')
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

        # pylint: disable-next=unbalanced-tuple-unpacking
        cmr_pixc_granules, cmr_orbit_granules \
            = _find_cmr_granules(cycle, passe, scene)
        # pylint: disable-next=unbalanced-tuple-unpacking
        grq_pixc_granules, grq_orbit_results \
            = _find_grq_granules(cycle, passe, scene)

        logger.debug('CMR PIXC results: %s', cmr_pixc_granules)
        logger.debug('CMR Orbit results: %s', cmr_orbit_granules)

        logger.debug('GRQ PIXC results: %s', grq_pixc_granules)
        logger.debug('GRQ Orbit results: %s', grq_orbit_results)

        to_ingest = (cmr_pixc_granules | cmr_orbit_granules) \
            - (grq_pixc_granules | grq_orbit_results)
        # Don't delete orbit files
        to_delete = grq_pixc_granules - cmr_pixc_granules

        logger.debug('To ingest: %s', to_ingest)
        logger.debug('To delete: %s', to_delete)

        ingest_jobs = _ingest_granules(to_ingest)
        _delete_grq_granules(to_delete)

        for job in ingest_jobs:
            job['product_id'] = body['product_id']
            jobs.append(job)

    jobset = validate_jobset({
        'jobs': jobs,
        'inputs': inputs
    })

    return jobset


def _find_cmr_granules(cycle, passe, scene) -> tuple[Granule, Granule]:
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
        f'{i:03}L,{i:03}R,' for i in range((scene * 2) - 1, (scene * 2) + 3)
    ])
    variables = {
        'tileParams': {
            'collectionConceptIds': [PIXC_CONCEPT_ID, PIXCVEC_CONCEPT_ID],
            'cycle': cycle,
            'passes': {
                '0': {
                    'pass': passe,
                    'tiles': tile_ids[:-1]
                }
            },
            'limit': 100
        },

        'orbitParams': {
            'collectionConceptId': XDF_ORBIT_CONCEPT_ID,
            'sortKey': '-end_date',
            'limit': 1
        }
    }

    body = {
        'query': query,
        'variables': variables
    }
    logger.debug('CMR request body: %s', str(body))

    response = requests.post(
        GRAPHQL_ENDPOINT,
        headers={'Authorization': f'Bearer {EDL_TOKEN}'},
        timeout=15,
        json=body
    )

    if not response.ok:
        raise RuntimeError('Experienced network error attempting to reach CMR')

    body = response.json()
    logger.debug('CMR response body: %s', str(body))

    tiles = body['data']['tiles']['items']
    orbit = body['data']['orbit']['items']

    results = []

    for dataset in (tiles, orbit):
        granules = set()

        for granule in dataset:
            s3_link = _find_s3_link(granule['relatedUrls'])
            if s3_link is None:
                logger.warning('No s3 link found: %s', granule['granuleUr'])
                continue

            granules.add(Granule(granule['granuleUr'], s3_link))
        results.append(granules)

    return tuple(results)


def _find_grq_granules(cycle, passe, scene) -> tuple[Granule, Granule]:
    collection_ids = ['L2_HR_PIXC', 'L2_HR_PIXCVec']
    tile_ids = [
        str(tile).rjust(3, '0') for tile in range(scene * 2 - 1, scene * 2 + 3)
    ]

    # pylint: disable-next=unexpected-keyword-arg
    pixc_results = grq_es_client.search(
        index='grq',
        size=100,
        body={
            'query': {
                'bool': {
                    'must': [
                        {'term': {'dataset_type.keyword': 'SDP'}},
                        {'terms': {'dataset.keyword': collection_ids}},
                        {'term': {'metadata.CycleID': f'{cycle:03}'}},
                        {'term': {'metadata.PassID': f'{passe:03}'}},
                        {'terms': {'metadata.TileID': tile_ids}}
                    ]
                }
            }
        }
    )

    # pylint: disable-next=unexpected-keyword-arg
    orbit_results = grq_es_client.search(
        index='grq',
        size=1,
        body={
            'query': {
                'bool': {
                    'must': [
                        {'term': {'dataset_type.keyword': 'AUX'}},
                        {'term': {'dataset.keyword': 'XDF_ORBIT_REV_FILE'}}
                    ]
                }
            },
            'sort': {'endtime': {'order': 'desc'}}
        }
    )

    logger.debug('PIXC grq results: %s', pixc_results)
    logger.debug('Orbit grq results: %s', orbit_results)

    output = []

    for dataset in (pixc_results, orbit_results):
        granules = set()

        for result in dataset['hits']['hits']:
            metadata = result['_source']['metadata']
            granules.add(Granule(metadata['id'], metadata['ISL_urls']))

        output.append(granules)

    return tuple(output)


def _find_s3_link(related_urls):
    for url in related_urls:
        logger.debug('Evaluating URL: %s', str(url))

        if not url['type'].startswith('GET DATA'):
            logger.debug('Skipping because GET DATA not found')
            continue

        if urlparse(url['url']).scheme.lower() == 's3':
            return url['url']

    logger.warning('No S3 links found')
    return None


def _ingest_granules(granules):
    jobs = []

    for granule in granules:
        filename = PurePath(urlparse(granule.url).path).name
        ingest_job_type.set_input_params(_gen_mozart_job_params(
            filename, granule.url
        ))

        job = ingest_job_type.submit_job(
            tag=f'ingest_file_otello__{granule.name}',
            publish_overwrite_ok=True
        )

        jobs.append({
            'job_id': job.job_id,
            'job_status': 'job-queued',
            'stage': STAGE
        })

    return jobs


def _delete_grq_granules(granules):
    grq_es_client.delete_by_query(index='grq', body={
        'query': {
            'ids': {'values': [granule.name for granule in granules]}
        }
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
