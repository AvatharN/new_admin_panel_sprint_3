from typing import Any, Dict, List


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
