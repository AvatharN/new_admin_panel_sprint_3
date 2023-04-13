import logging
import json

from typing import Dict, Generator, List, Any

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from elasticsearch.exceptions import ConnectionError, ConnectionTimeout, TransportError
from backoff import on_exception, expo


class ElasticSearchSender:
    """
    Class for sending data to ES. Context managed.
    """

    def __init__(self, host: str, port: int, index_name: str, scheme: str = 'http'):
        self.host = host
        self.port = port
        self.scheme = scheme
        self.client = None
        self.index_name = index_name
        self.index_body = None

    def get_index_from_file(self):
        with open('index.json') as file:
            self.index_body = json.load(file)
            return self.index_body

    def __enter__(self):
        self.connect()
        return self.client

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def connect(self):
        self.client = Elasticsearch([{'host': self.host,
                                      'port': self.port,
                                      'scheme': self.scheme}])

    def _generate_actions(self, data: List[Dict]) -> Generator[Dict, Any, Any]:
        """
        :param data:
        :return: Dict that match index schema
        """
        for row in data:
            doc = {
                'id': row['id'],
                '_id': row['id'],
                'imdb_rating': row['rating'],
                'genre': [g['name'] for g in row['genre'].values()],
                'title': row['title'],
                'description': row['description'],
                'director': [d['full_name'] for d in row['director'].values()] if row['director'] else [],
                'actors_names': [a['full_name'] for a in row['actor'].values()],
                'writers_names': [w['full_name'] for w in row['writer'].values()],
                'actors': [{'id': a['id'], 'name': a['full_name']} for a in row['actor'].values()],
                'writers': [{'id': w['id'], 'name': w['full_name']} for w in row['writer'].values()]
            }
            logging.log(logging.INFO, doc)
            yield doc

    @on_exception(expo, (ConnectionError, ConnectionTimeout, TransportError), max_tries=500)
    def send_data(self, data: List[Dict]):
        """
        Send bulk data to ElasticSearch. Context Managed, no need to wrap it 'with'
        :param data:
        :return:
        """
        with self as client:
            for action in streaming_bulk(client=client,
                                         index=self.index_name,
                                         actions=self._generate_actions(data)
                                         ):
                logging.log(logging.DEBUG, action)

    @on_exception(expo, (ConnectionError, ConnectionTimeout, TransportError), max_tries=500)
    def create_index(self) -> None:
        """
        Trying to create index, ignores error 400 for case if it's already created
        Context Managed, no need to wrap it 'with'
        :return:
        """
        with self as client:
            client.indices.create(
                index=self.index_name,
                body=self.get_index_from_file(),
                ignore=400,
            )
