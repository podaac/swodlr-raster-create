'''Shared utilities for raster-create lambdas'''
import json
import logging
import sys
from importlib import resources
from os import getenv
from pathlib import PurePath
from tempfile import NamedTemporaryFile
from typing import Callable
from urllib.parse import urljoin

import boto3
import fastjsonschema
from otello.mozart import Mozart
from requests import Session

import podaac.swodlr_raster_create


class Utils:
    '''Utility functions implemented as a singleton'''
    APP_NAME = 'swodlr'
    SSM_PATH = f'/service/{APP_NAME}/raster-create/'

    def __init__(self):
        self.env = getenv('SWODLR_ENV', 'prod')

        if self.env == 'prod':
            self._load_params_from_ssm()
        else:
            from dotenv import load_dotenv  # noqa: E501 # pylint: disable=import-outside-toplevel
            load_dotenv()

    def _load_params_from_ssm(self):
        ssm = boto3.client('ssm')

        parameters = []
        next_token = None
        while True:
            kwargs = {'NextToken': next_token} \
                if next_token is not None else {}
            res = ssm.get_parameters_by_path(
                Path=Utils.SSM_PATH,
                WithDecryption=True,
                **kwargs
            )

            parameters.extend(res['Parameters'])
            if 'NextToken' in res:
                next_token = res['NextToken']
            else:
                break

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
                # pylint: disable=consider-using-with
                cert_file = NamedTemporaryFile('w', delete=False)
                cert_file.write(ca_cert)
                cert_file.flush()
                session.verify = cert_file.name

            self._session = session  # noqa: E501 # pylint: disable=attribute-defined-outside-init

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
            host = self.get_param('sds_host')
            path = self.get_param('sds_grq_es_path')
            index = self.get_param('sds_grq_es_index')

            search_path = str(PurePath(host, path, index, '_search'))
            es_path = urljoin(host, search_path)
            self._grq_es_path = es_path  # noqa: E501 # pylint: disable=attribute-defined-outside-init

        session = self._get_session()
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

    def get_logger(self, name):
        '''
        Creates a logger for a requestor with a global log level defined from
        parameters
        '''
        logger = logging.getLogger(name)

        log_level = getattr(logging, self.get_param('log_level')) \
            if self.get_param('log_level') is not None else logging.INFO
        logger.setLevel(log_level)
        return logger

    def load_json_schema(self, name):
        '''
        Load a json schema from the schema folder and return the compiled
        schema
        '''
        schemas = resources.files(podaac.swodlr_raster_create) \
            .joinpath('schemas')
        schema_resource = schemas.joinpath(f'{name}.json')

        if not schema_resource.is_file():
            raise RuntimeError('Schema not found')

        with schema_resource.open('r', encoding='utf-8') as f:
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


# Silence the linters
mozart_client: Mozart
get_logger: Callable[[str], logging.Logger]
get_param: Callable[[str, str], str]
search_datasets: Callable[[str], str]
load_json_schema: Callable[[str], Callable]

sys.modules[__name__] = Utils()
