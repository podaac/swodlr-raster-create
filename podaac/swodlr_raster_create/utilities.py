'''Shared utilities for raster-create lambdas'''
from importlib import resources
import json
from pathlib import Path, PurePath
from urllib.parse import urljoin
import fastjsonschema
from otello.mozart import Mozart

import podaac.swodlr_raster_create
from podaac.swodlr_common.utilities import BaseUtilities


class Utilities(BaseUtilities):
    '''Utility functions implemented as a singleton'''
    APP_NAME = 'swodlr'
    SERVICE_NAME = 'raster-create'
    SCHEMAS_PATH = Path(__file__, '..', 'schemas')

    def __init__(self):
        super().__init__(Utilities.APP_NAME, Utilities.SERVICE_NAME)

    def load_json_schema(self, name):
        schemas = resources.files(podaac.swodlr_raster_create) \
            .joinpath('schemas')
        schema_resource = schemas.joinpath(f'{name}.json')

        if not schema_resource.is_file():
            return super().load_json_schema(name)

        with schema_resource.open('r', encoding='utf-8') as schema_json:
            return fastjsonschema.compile(json.load(schema_json))

    def search_datasets(self, dataset_id, wildcard=True):
        '''
        Searches for datasets by id using a lazily created session, supporting
        wildcard searches by default
        '''
        if not hasattr(self, '_grq_es_path'):
            host = self.get_param('sds_host')
            path = self.get_param('sds_grq_es_path')
            index = self.get_param('sds_grq_es_index')

            search_path = str(PurePath(host, path, index, '_search'))
            es_path = urljoin(host, search_path)
            self._grq_es_path = es_path  # noqa: E501 # pylint: disable=attribute-defined-outside-init

        session = self._get_sds_session()
        es_path = self._grq_es_path
        query_type = 'wildcard' if wildcard else 'term'

        res = session.get(es_path, json={
            'size': 1,
            'query': {
                query_type: {
                    'id.keyword': dataset_id
                }
            }
        })

        body = res.json()
        if len(body['hits']['hits']) == 0:
            return None

        return body['hits']['hits'][0]['_source']

    @property
    def mozart_client(self):
        '''
        Lazily creates a Mozart client
        '''
        if not hasattr(self, '_mozart_client'):
            host = self.get_param('sds_host')
            username = self.get_param('sds_username')
            cfg = {
                'host': host,
                'auth': True,
                'username': username
            }

            # pylint: disable=attribute-defined-outside-init
            self._mozart_client = Mozart(cfg, session=self._get_sds_session())

        return self._mozart_client


utils = Utilities()
