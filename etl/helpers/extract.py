from typing import Dict, Generator, List, Set, Any
import logging

import psycopg2
from backoff import on_exception, expo

from .stateman import StateManager


class PostgresExtractor:
    def __init__(self, dns: Dict[str, str], state_man: StateManager):
        self.dns = dns
        self.state_manager = state_man
        self.connection = None
        self.last_modified = self.state_manager.load_state()

    def load_state(self):
        self.last_modified = self.state_manager.load_state()

    def __enter__(self):
        self.connect()
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    @on_exception(expo, psycopg2.OperationalError, max_tries=10)
    def connect(self):
        self.connection = psycopg2.connect(**self.dns)

    def execute_query(self, query: str, batch_size=100) -> Generator[Dict, Any, Any]:
        """
        Context managed cursor and connection executor
        :param query: SQL query
        :param batch_size:
        :return:
        """
        with self as conn:
            with conn.cursor() as cursor:
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
