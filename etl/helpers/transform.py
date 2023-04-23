from typing import Any, Dict, List


def transform(rows: List[Dict:Any]):
    """
    Transformer function that morph Postgres-object to ElasticSearch-bulk ready object
    :param rows:
    :return:
    """
    movies = {}
    genres = {}
    roles = {}
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

        if genre_id not in genres:
            genre = {
                "id": genre_id,
                "name": genre_name,
                "films": {}
            }
            genres[genre_id] = genre

        if person_id not in roles:
            person = {
                "id": person_id,
                "name": person_name,
                "films": {}
            }
            roles[person_id] = person

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
            if id not in roles[person_id]['films'].keys():
                roles[person_id]['films'][id] = {"id": id, "title": title}
        if genre_name:
            genre = {
                "name": genre_name,
                "id": genre_id
            }
            if genre_name not in movie['genre'].keys():
                movie['genre'][genre_id] = genre
            if id not in genres[genre_id]['films'].keys():
                genres[genre_id]['films'][id] = {"id": id, "title": title}

    transformed_movies = [movie for movie in movies.values()]
    transformed_roles = [role for role in roles.values()]
    transformed_genres = [genre for genre in genres.values()]

    return {"movies": transformed_movies, "roles": transformed_roles, "genres": transformed_genres}
