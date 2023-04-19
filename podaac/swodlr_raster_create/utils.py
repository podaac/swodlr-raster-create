'''Shared utilities for ingest-to-sds lambdas'''
from importlib import resources
import json
import sys
from time import sleep
from typing import Any, Callable
import logging
from os import getenv
from pathlib import Path, PurePath
from tempfile import mkstemp
from urllib.parse import urljoin

import boto3
from dotenv import load_dotenv
import fastjsonschema
from mypy_boto3_sqs.service_resource import Queue
from otello.mozart import Mozart, Job, JobType
from requests import Session


load_dotenv()


class Utils:
    '''Utility functions implemented as a singleton'''
    APP_NAME = 'swodlr'
    SSM_PATH = f'/service/{APP_NAME}/raster_create/'

    def __init__(self):
        self.env = getenv('SWODLR_ENV', 'prod')

        if self.env == 'prod':
            self._load_params_from_ssm()

    def _load_params_from_ssm(self):
        ssm = boto3.client('ssm')
        parameters = ssm.get_parameters_by_path(
            path=Utils.SSM_PATH,
            with_decryption=True
        )['Parameters']

        self._ssm_parameters = {}

        for param in parameters:
            name = param['Name'].removeprefix(self.SSM_PATH)
            self._ssm_parameters[name] = param['Value']

    def _get_session(self):
        '''
        Lazily create authenticated session for internal use

        CAUTION: THE SESSION OBJECT IS AUTHENTICATED. DO NOT USE THIS SESSION
        OUTSIDE OF THIS UTILITY CLASS OR CREDENTIALS MAY LEAK
        '''
        if not hasattr(self, '_session'):
            ca_cert = self.get_param('sds_ca_cert')
            username = self.get_param('sds_username')
            password = self.get_param('sds_password')

            session = Session()
            session.auth = (username, password)

            if ca_cert is not None:
                cert_file, cert_path = mkstemp(text=True)
                cert_file.write(ca_cert)
                cert_file.flush()
                session.verify = cert_path

            self._session = session

        return self._session

    def get_param(self, name):
        '''
        Retrieves a parameter from SSM or the environment depending on the
        environment
        '''
        if self.env == 'prod':
            return self._ssm_parameters.get(name)

        return getenv(f'{self.APP_NAME.upper()}_{name}')

    def search_datasets(self, dataset_id, wildcard=True):
        '''
        Searches for datasets by id using a lazily created session, supporting
        wildcard searches by default
        '''
        if not hasattr(self, '_grq_es_path'):
            host  = self.get_param('sds_host')
            path  = self.get_param('sds_grq_es_path')
            index = self.get_param('sds_grq_es_index')

            search_path = str(PurePath(host, path, index, '_search'))
            es_path = urljoin(host, search_path)
            self._grq_es_path = es_path

        session = self._get_session()
        es_path = self._grq_es_path
        query_type = 'wildcard' if wildcard else 'term'

        res = session.get(es_path, data = {
            'size': 1,
            'query': {
                query_type: {
                    'dataset.keyword': dataset_id
                }
            }
        })

        body = res.json()
        if len(body['hits']['hits']) == 0:
            return None
        
        return body['hits']['hits'][0]['_source']

    def load_json_schema(self, name):
        schemas = resources.files('podaac.swodlr_raster_create.schemas')
        schema_resource = schemas.joinpath(f'{name}.json')

        if not schema_resource.is_file():
            raise RuntimeError('Schema not found')

        with schema_resource.open('r') as f:
            return fastjsonschema.compile(json.load(f))

    @property
    def mozart_client(self):
        '''
        Lazily creates a Mozart client
        '''
        if not hasattr(self, '_mozart_client'):
            host = self.get_param('sds_host')
            username = self.get_param('sds_username')
            password = self.get_param('sds_password')

            cfg = {
                'host': host,
                'auth': True,
                'username': username,
                'password': password
            }

            # pylint: disable=attribute-defined-outside-init
            self._mozart_client = Mozart(cfg, session=self._get_session())

        return self._mozart_client

    @property
    def update_queue(self):
        '''
        Lazily creates the db update queue resource
        '''
        if not hasattr(self, '_update_queue'):
            update_queue_url = self.get_param('update_queue_url')

            sqs = boto3.resource('sqs')
            self._update_queue = sqs.Queue(update_queue_url)
        
        return self._update_queue

    @property
    def update_topic(self):
        if not hasattr(self, '_update_topic'):
            update_topic_arn = self.get_param('update_topic_arn')

# Silence the linters
mozart_client: Mozart
update_queue: Queue
get_param: Callable[[str, str], str]
search_datasets: Callable[[str], str]
load_json_schema: Callable[[str], Callable]


sys.modules[__name__] = Utils()
