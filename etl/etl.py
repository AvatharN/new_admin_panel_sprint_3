import datetime
import logging
import os

from time import sleep
from typing import Any, Dict, Generator, List, Set, AnyStr

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from elasticsearch.exceptions import ConnectionError, ConnectionTimeout, TransportError
import psycopg2
from backoff import on_exception, expo
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)

load_dotenv()

PSQL_SETTINGS = {'dbname': os.environ.get('POSTGRES_DB'),
                 'user': os.environ.get('POSTGRES_USER'),
                 'password': os.environ.get('POSTGRES_PASSWORD'),
                 'host': os.environ.get('POSTGRES_HOST'),
                 'port': os.environ.get('POSTGRES_PORT')}

ELASTIC_SETTINGS = {'host': os.environ.get('ELASTIC_HOST'),
                    'port': int(os.environ.get('ELASTIC_PORT'))}

STATEFILENAME = os.environ.get('STATE_FILENAME')

TIMEOUT = int(os.environ.get('TIMEOUT'))


def transform(rows: List[Dict:Any]):
    """
    Transformer function that morph Postgres-object to ElasticSearch-bulk ready object
    :param rows:
    :return:
    """
    movies = {}
    for row in rows:
        id = row[0]
        title = row[1]
        description = row[2]
        rating = row[3]
        showtype = row[4]
        person_name = row[5]
        person_id = row[6]
        role = row[7]
        genre_id = row[8]
        genre_name = row[9]

        if id not in movies:
            movie = {
                "id": id,
                "title": title,
                "description": description,
                "rating": rating,
                "director": {},
                "actor": {},
                "writer": {},
                "genre": {},
                "type": showtype
            }
            movies[id] = movie
        movie = movies[id]
        if person_name and role:
            person = {
                "full_name": person_name,
                "id": person_id
            }
            if person_id not in movie[role.lower()].keys():
                movie[role.lower()][person_id] = person
        if genre_name:
            genre = {
                "name": genre_name,
                "id": genre_id
            }
            if genre_name not in movie['genre'].keys():
                movie['genre'][genre_id] = genre
    transformed_data = [movie for movie in movies.values()]
    return transformed_data


class StateManager:
    def __init__(self, state_file: AnyStr):
        """
        :param state_file: file where state will be saved
        """
        self.state_file = state_file
        self.default = datetime.datetime(1970, 1, 1).strftime('%Y-%m-%d %H:%M:%S')

    def load_state(self):
        """
        :return: datetime loaded from file. If file not exist, create it with default datetime
        """
        try:
            with open(self.state_file, 'r') as f:
                state = f.read()
                if state:
                    a = state.strip()
                    logging.log(logging.INFO, f"loaded:  {datetime.datetime.strptime(a, '%Y-%m-%d %H:%M:%S')}")
                    return datetime.datetime.strptime(a, '%Y-%m-%d %H:%M:%S')
                else:
                    return self.default
        except FileNotFoundError:
            self.save_state()
            return self.default

    def save_state(self):
        """
        save current time to file
        :return:
        """
        with open(self.state_file, 'w') as f:
            logging.log(logging.INFO, f"saved:  {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            f.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


class PostgresExtractor:
    def __init__(self, dns: Dict[str, str], state_man: StateManager):
        self.dns = dns
        self.state_manager = state_man
        self.connection = None
        self.last_modified = self.state_manager.load_state()

    def load_state(self):
        self.last_modified = self.state_manager.load_state()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    @on_exception(expo, psycopg2.OperationalError, max_tries=10)
    def connect(self):
        self.connection = psycopg2.connect(**self.dns)

    def execute_query(self, query: str, batch_size=100) -> Generator[Dict]:
        """
        Context managed cursor executor
        :param query: SQL query
        :param batch_size:
        :return:
        """
        if not self.connection:
            self.connect()
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            while True:
                results = cursor.fetchmany(batch_size)
                if not results:
                    break
                for row in results:
                    yield row

    def get_modified_films(self) -> Set:
        """
        Get set of film_works where genre or person was modified
        :return:
        """
        edited_films = f"SELECT DISTINCT fw.id \
                        FROM content.film_work AS fw \
                        LEFT JOIN content.person_film_work AS pfw ON pfw.film_work_id = fw.id \
                        LEFT JOIN content.person AS pr ON pr.id = pfw.person_id \
                        LEFT JOIN content.genre_film_work AS gfw ON gfw.film_work_id = fw.id \
                        LEFT JOIN content.genre AS gn ON gn.id = gfw.genre_id \
                        WHERE GREATEST(fw.modified, pr.modified, gn.modified) > '{self.last_modified}' \
                        ORDER BY fw.id"
        rows = self.execute_query(edited_films)
        extracted = set()
        if not rows:
            logging.info('No new modified filmworks found')
            return set()
        for row in rows:
            extracted.add(row[0])
        return extracted

    def get_films_data(self) -> List[Dict]:
        """
        Get all needed for Elasticsearch entries for modified filmworks
        :return:
        """
        self.load_state()
        ids = self.get_modified_films()
        if not ids:
            return []
        id_strings = [str(id) for id in ids]
        id_strings = [id.replace("'", "''") for id in id_strings]
        id_string = ",".join([f"'{id}'" for id in id_strings])
        sql_query = f"""
                SELECT
                    fw.id,
                    fw.title,
                    fw.description,
                    fw.rating,
                    fw.type,
                    pr.full_name AS person_name,
                    pr.id,
                    pfw.role,
                    gn.id,
                    gn.name AS genre_name
                FROM
                    content.film_work fw
                    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                    LEFT JOIN content.person pr ON pr.id = pfw.person_id
                    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                    LEFT JOIN content.genre gn ON gn.id = gfw.genre_id
                WHERE
                    fw.id IN ({id_string})
                ORDER BY fw.id
            """
        rows = self.execute_query(sql_query)
        extracted = []
        for row in rows:
            extracted.append(row)
        return extracted


class ElasticSearchSender:
    """
    Class for sending data to ES
    """

    def __init__(self, host: str, port: int, index_name: str):
        self.es = Elasticsearch([{'host': host, 'port': port, 'scheme': 'http'}])
        self.index_name = index_name

    def _generate_actions(self, data: List[Dict]) -> Generator[Dict]:
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
            yield doc

    @on_exception(expo, (ConnectionError, ConnectionTimeout, TransportError), max_tries=500)
    def send_data(self, data: List[Dict]):
        """
        Send bulk data to ElasticSearch
        :param data:
        :return:
        """
        for action in streaming_bulk(client=self.es,
                                     index=self.index_name,
                                     actions=self._generate_actions(data)
                                     ):
            logging.log(logging.DEBUG, action)

    @on_exception(expo, (ConnectionError, ConnectionTimeout, TransportError), max_tries=500)
    def create_index(self) -> None:
        """
        Trying to create index, ignores error 400 for case if it's already created
        :return:
        """
        self.es.indices.create(
            index='movies',
            body={
                "mappings": {"dynamic": "strict",
                             "properties": {
                                 "actors": {
                                     "dynamic": "strict",
                                     "properties":
                                         {"id": {"type": "keyword"},
                                          "name": {"analyzer": "ru_en", "type": "text"}},
                                     "type": "nested"},
                                 "actors_names": {"analyzer": "ru_en", "type": "text"},
                                 "description": {"analyzer": "ru_en", "type": "text"},
                                 "director": {"analyzer": "ru_en", "type": "text"},
                                 "genre": {"type": "keyword"},
                                 "id": {"type": "keyword"},
                                 "imdb_rating": {"type": "float"},
                                 "title": {"analyzer": "ru_en",
                                           "fields": {"raw": {"type": "keyword"}},
                                           "type": "text"},
                                 "writers": {"dynamic": "strict",
                                             "properties":
                                                 {"id": {"type": "keyword"},
                                                  "name": {"analyzer": "ru_en", "type": "text"}},
                                             "type": "nested"},
                                 "writers_names": {"analyzer": "ru_en", "type": "text"}}},
                "settings": {"analysis": {"analyzer": {"ru_en": {
                    "filter": ["lowercase", "english_stop", "english_stemmer", "english_possessive_stemmer",
                               "russian_stop", "russian_stemmer"], "tokenizer": "standard"}}, "filter": {
                    "english_possessive_stemmer": {"language": "possessive_english", "type": "stemmer"},
                    "english_stemmer": {"language": "english", "type": "stemmer"},
                    "english_stop": {"stopwords": "_english_", "type": "stop"},
                    "russian_stemmer": {"language": "russian", "type": "stemmer"},
                    "russian_stop": {"stopwords": "_russian_", "type": "stop"}}},
                    "refresh_interval": "1s"}
            },
            ignore=400,
        )


if __name__ == '__main__':
    state_manager = StateManager(STATEFILENAME)
    pse = PostgresExtractor(PSQL_SETTINGS, state_manager)
    es_sender = ElasticSearchSender(ELASTIC_SETTINGS['host'],
                                    ELASTIC_SETTINGS['port'],
                                    index_name='movies')
    es_sender.create_index()
    while True:
        movie_data = pse.get_films_data()
        state_manager.save_state()
        tformed_data = transform(movie_data)
        es_sender.send_data(tformed_data)
        sleep(TIMEOUT)
