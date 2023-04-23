import logging
import json

from typing import Dict, Generator, List, Any

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, BulkIndexError
from elasticsearch.exceptions import ConnectionError, ConnectionTimeout, TransportError
from backoff import on_exception, expo


class ElasticSearchSender:
    """
    Class for sending data to ES. Context managed.
    """

    def __init__(self, host: str, port: int, scheme: str = 'http'):
        self.index_name = None
        self.host = host
        self.port = port
        self.scheme = scheme
        self.client = None
        self.index_body = None

    def get_index_from_file(self, entity: str):
        with open(f'{entity}_index.json') as file:
            logging.log(logging.INFO, f"Trying to open {entity+'_index.json'}")
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

    def _generate_movie_actions(self, data: List[Dict]) -> Generator[Dict, Any, Any]:
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

    def _generate_role_actions(self, data: List[Dict]) -> Generator[Dict, Any, Any]:
        for row in data:
            if row['id'] and row['name']:
                doc = {
                    '_id': row['id'],
                    'id': row['id'],
                    'name': row['name'],
                    'films': [{"id": f["id"], "title": f["title"]} for f in row["films"].values()]
                }
                logging.log(logging.INFO, doc)
                yield doc

    def _generate_genre_actions(self, data: List[Dict]) -> Generator[Dict, Any, Any]:
        for row in data:
            if row['id'] and row['name']:
                doc = {
                    '_id': row['id'],
                    'id': row['id'],
                    'name': row['name'],
                    'films': [{"id": f["id"], "title": f["title"]} for f in row["films"].values()]
                }
                logging.log(logging.INFO, doc)
                yield doc

    @on_exception(expo, (ConnectionError, ConnectionTimeout, TransportError), max_tries=500)
    def send_data(self, data_dict: Dict[str, List[Dict]]):
        """
        Send bulk data to ElasticSearch. Context Managed, no need to wrap it 'with'
        :param data:
        :return:
        """
        for entity, data in data_dict.items():
            self.create_index(entity)
            with self as client:
                actions = {
                    "movies": self._generate_movie_actions,
                    "roles": self._generate_role_actions,
                    "genres": self._generate_genre_actions
                }[entity](data)

                try:
                    for action in streaming_bulk(client=client,
                                                 index=entity,
                                                 actions=actions
                                                 ):
                        logging.log(logging.DEBUG, action)
                except BulkIndexError as e:
                    logging.error(f"BulkIndexError: {e}")
                    logging.error(f"Failed documents: {e.errors}")

    @on_exception(expo, (ConnectionError, ConnectionTimeout, TransportError), max_tries=500)
    def create_index(self, index_name: str) -> None:
        """
        Trying to create index, ignores error 400 for case if it's already created
        Context Managed, no need to wrap it 'with'
        :return:
        """
        with self as client:
            client.indices.create(
                index=index_name,
                body=self.get_index_from_file(index_name),
                ignore=400,
            )
