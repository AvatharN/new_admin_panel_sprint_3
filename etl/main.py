import logging

from time import sleep

from helpers.stateman import StateManager
from helpers.extract import PostgresExtractor
from helpers.transform import transform
from helpers.load import ElasticSearchSender
from settings import Settings

logging.basicConfig(level=logging.INFO)
logging.getLogger('backoff').addHandler(logging.StreamHandler())

settings = Settings(_env_file='.env')

if __name__ == '__main__':
    state_manager = StateManager(settings.state_filename)

    es_sender = ElasticSearchSender(settings.elastic.host,
                                    settings.elastic.port,
                                    index_name=settings.elastic.index)
    pse = PostgresExtractor(settings.postgres.dict(), state_manager)
    es_sender.create_index()
    while True:
        movie_data = pse.get_films_data()
        state_manager.save_state()
        tformed_data = transform(movie_data)
        es_sender.send_data(tformed_data)
        sleep(settings.timeout)
