from functions_db import *


def create_tables(connection):
    try:
        create_film_table = """
        CREATE TABLE film (
          show_id INT AUTO_INCREMENT PRIMARY KEY,
          title VARCHAR(80),
          type VARCHAR(50),
          country VARCHAR(50),
          date_added DATE,
          release_year INT,
          rating VARCHAR(50),
          duration VARCHAR(50),
          listed_in VARCHAR(50),
          description VARCHAR(250)
        );
        """

        create_directors_table = """
        CREATE TABLE directors (
          id INT AUTO_INCREMENT PRIMARY KEY,
          name VARCHAR(50) NULL
        );
        """

        create_actors_table = """
        CREATE TABLE actors (
          id INT AUTO_INCREMENT PRIMARY KEY,
          name VARCHAR(50) NULL
        );
        """

        create_director_participation_table = """
        CREATE TABLE director_participation (
          id INT AUTO_INCREMENT PRIMARY KEY,
          show_id INT NOT NULL,
          id_director INT NOT NULL
        );
        """

        create_actor_participation_table = """
        CREATE TABLE actor_participation (
          id INT AUTO_INCREMENT PRIMARY KEY,
          show_id INT NOT NULL,
          id_actor INT NOT NULL
        );
        """

        alter_director_participation = """
        ALTER TABLE director_participation
        ADD FOREIGN KEY (show_id) REFERENCES film(show_id) ON DELETE CASCADE,
        ADD FOREIGN KEY (id_director) REFERENCES directors(id) ON DELETE CASCADE;
        """
        alter_actor_participation = """
        ALTER TABLE actor_participation
        ADD FOREIGN KEY (show_id) REFERENCES film(show_id) ON DELETE CASCADE,
        ADD FOREIGN KEY (id_actor) REFERENCES actors(id) ON DELETE CASCADE;
        """

        # creation tables
        execute_query(connection, create_film_table)
        execute_query(connection, create_directors_table)
        execute_query(connection, create_actors_table)
        execute_query(connection, create_director_participation_table)
        execute_query(connection, create_actor_participation_table)

        # adding foreign keys and set constraints
        execute_query(connection, alter_director_participation)
        execute_query(connection, alter_actor_participation)

        connection.commit()
        print("Tabelle create correttamente.")
    except Exception as e:
        print(f"Errore nella creazione delle tabelle: {e}")