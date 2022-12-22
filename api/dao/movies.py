from api.data import popular, goodfellas
import os
from api.exceptions.notfound import NotFoundException


class MovieDAO:
    """The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j."""

    def __init__(self, driver):
        self.driver = driver

    def all(self,
            sort,
            order,
            limit=6,
            skip=0,
            user_id=os.environ.get("USER_ID")):
        """This method should return a paginated list of movies ordered by the
        `sort` parameter and limited to the number passed as `limit`.  The
        `skip` variable should be used to skip a certain number of rows.

        If a user_id value is suppled, a `favorite` boolean property
        should be returned to signify whether the user has added the
        movie to their "My Favorites" list.
        """

        def get_movies(tx, sort, order, limit, skip, user_id):
            # Get User favorites
            favorites = self.get_user_favorites(tx, user_id)

            # Define the cypher statement
            cypher = """
                MATCH (m:Movie)
                WHERE exists(m.`{0}`)
                RETURN m {{
                    .*,
                    favorite: m.tmdbId IN $favorites
                }} AS movie
                ORDER BY m.`{0}` {1}
                SKIP $skip
                LIMIT $limit
            """.format(sort, order)

            # Run the statement within the transaction
            # passed as the first argument
            result = tx.run(
                cypher,
                limit=limit,
                skip=skip,
                user_id=user_id,
                favorites=favorites)
            # Extract a list of Movies from the Result
            return [row.value("movie") for row in result]

        with self.driver.session() as session:
            return session.execute_read(get_movies, sort, order, limit, skip,
                                        user_id)

    def get_by_genre(self,
                     name,
                     sort="title",
                     order="ASC",
                     limit=6,
                     skip=0,
                     user_id=None):
        """This method should return a paginated list of movies that have a
        relationship to the supplied Genre.

        Results should be ordered by the `sort` parameter, and in the direction
        specified in the `order` parameter.
        Results should be limited to the number passed as `limit`.
        The `skip` variable should be used to skip a certain number of rows.

        If a user_id value is suppled, a `favorite` boolean property should be
        returned to signify whether the user has added the movie to their
        "My Favorites" list.
        """

        def all(self):
            # Define a unit of work to Get a list of Genres
            def get_movies(tx):
                result = tx.run("""
                    MATCH (g:Genre)
                    WHERE g.name <> '(no genres listed)'
                    CALL {
                        WITH g
                        MATCH (g)<-[:IN_GENRE]-(m:Movie)
                        WHERE m.imdbRating IS NOT NULL AND m.poster IS NOT NULL
                        RETURN m.poster AS poster
                        ORDER BY m.imdbRating DESC LIMIT 1
                    }
                    RETURN g {
                        .*,
                        movies: size((g)<-[:IN_GENRE]-(:Movie)),
                        poster: poster
                    } AS genre
                    ORDER BY g.name ASC
                """)

                return [g.value(0) for g in result]

            # Open a new session
            with self.driver.session() as session:
                # Execute within a Read Transaction
                return session.execute_read(get_movies)

    def get_for_actor(self,
                      id,
                      sort="title",
                      order="ASC",
                      limit=6,
                      skip=0,
                      user_id=None):
        """This method should return a paginated list of movies that have an
        ACTED_IN relationship to a Person with the id supplied.

        Results should be ordered by the `sort` parameter, and in the direction
        specified in the `order` parameter.
        Results should be limited to the number passed as `limit`.
        The `skip` variable should be used to skip a certain number of rows.

        If a user_id value is suppled, a `favorite` boolean property should be
        returned to signify whether the user has added the movie to their
        "My Favorites" list.
        """
        # TODO: Get Movies for an Actor
        # The Cypher string will be formated so remember to
        # escape the braces: {{tmdbId: $id}}
        # MATCH (:Person {tmdbId: $id})-[:ACTED_IN]->(m:Movie)

        return popular[skip:limit]

    def get_for_director(self,
                         id,
                         sort="title",
                         order="ASC",
                         limit=6,
                         skip=0,
                         user_id=None):
        """This method should return a paginated list of movies that have an
        DIRECTED relationship to a Person with the id supplied.

        Results should be ordered by the `sort` parameter, and in the direction
        specified in the `order` parameter.
        Results should be limited to the number passed as `limit`.
        The `skip` variable should be used to skip a certain number of rows.

        If a user_id value is suppled, a `favorite` boolean property should be
        returned to signify whether the user has added the movie to their
        "My Favorites" list.
        """
        # TODO: Get Movies directed by a Person
        # The Cypher string will be formated so remember to escape
        # the braces: {{name: $name}}
        # MATCH (:Person {tmdbId: $id})-[:DIRECTED]->(m:Movie)

        return popular[skip:limit]

    def find_by_id(self, id, user_id=None):
        """This method find a Movie node with the ID passed as the `id`
        parameter. Along with the returned payload, a list of actors,
        directors, and genres should be included. The number of incoming RATED
        relationships should also be returned as `ratingCount`

        If a user_id value is suppled, a `favorite` boolean property
        should be returned to signify whether the user has added the
        movie to their "My Favorites" list.
        """
        # TODO: Find a movie by its ID
        # MATCH (m:Movie {tmdbId: $id})

        return goodfellas

    def get_similar_movies(self, id, limit=6, skip=0, user_id=None):
        """This method should return a paginated list of similar movies to the
        Movie with the id supplied.  This similarity is calculated by finding
        movies that have many first degree connections in common: Actors,
        Directors and Genres.

        Results should be ordered by the `sort` parameter, and in the direction
        specified in the `order` parameter.
        Results should be limited to the number passed as `limit`.
        The `skip` variable should be used to skip a certain number of rows.

        If a user_id value is suppled, a `favorite` boolean property should be
        returned to signify whether the user has added the movie to their
        "My Favorites" list.
        """
        # TODO: Get similar movies from Neo4j

        return popular[skip:limit]

    def get_user_favorites(self, tx, user_id):
        """This function should return a list of tmdbId properties for the
        movies that the user has added to their 'My Favorites' list."""
        if user_id is None:
            return []

        result = tx.run(
            """
            MATCH (u:User {userId: $userId})-[:HAS_FAVORITE]->(m)
            RETURN m.tmdbId AS id
        """,
            userId=user_id)

        return [record.get("id") for record in result]
