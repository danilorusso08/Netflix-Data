import mysql.connector
from mysql.connector import Error
import pandas as pd


def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


def create_database(connection, name):
    cursor = connection.cursor()
    try:
        query = f"CREATE DATABASE {name}"
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")


# creo la connessione all'interno
def create_db(db_name):
    connection = create_db_connection('localhost', 'root', '', '')
    cursor = connection.cursor()
    try:
        cursor.execute(f"CREATE DATABASE {db_name}")
        connection.commit()
    except Exception as e:
        print(f"Errore durante la creazione del database {db_name}: {e}")
    finally:
        cursor.close()
        connection.close()


def create_db_input():
    host_name = "localhost"
    user_name = "root"
    user_password = ""

    db_name = input("Inserisci il nome del database: ")

    connection = create_server_connection(host_name, user_name, user_password)

    execute_query(connection, f"DROP DATABASE IF EXISTS {db_name}")
    create_database(connection, db_name)

    connection.close()

    return db_name


def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print(f"MySQL Database connection successful to {db_name}")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


def execute_query(connection, query, params=None):
    cursor = connection.cursor()
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")
    finally:
        cursor.close()


def read_query(connection, query, /, *, dictionary=False):
    if dictionary:
        cursor = connection.cursor(dictionary=True)
    else:
        cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")


def execute_list_query(connection, sql, val):
    cursor = connection.cursor()
    try:
        cursor.executemany(sql, val)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")
    finally:
        cursor.close()


def delete_show(connection):
    try:
        show_id = input("Enter the show ID you want to delete: ")
        delete_query = """
        DELETE FROM film
        WHERE show_id = %s
        """
        execute_query(connection, delete_query, (show_id,))
    except Exception as e:
        print(f"Error deleting show: {e}")


# funzioni per queries specifiche

def show_top_actors(connection):
    try:
        cursor = connection.cursor()
        query = """
        SELECT a.name, COUNT(ap.show_id) AS show_count
        FROM actors a
        JOIN actor_participation ap ON a.id = ap.id_actor
        WHERE a.name IS NOT NULL
        GROUP BY a.id
        ORDER BY show_count DESC
        LIMIT 10
        """
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results)
        df.columns = ['name', 'show_count']
        print(df)
        #for row in results:  #senza usare pandas
        #print(row)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()


def show_spielberg_shows(connection):
    try:
        cursor = connection.cursor()

        query = """
        SELECT DISTINCT f.title
        FROM film f
        JOIN director_participation dp ON f.show_id = dp.show_id
        JOIN directors d ON dp.id_director = d.id
        WHERE d.name = 'Steven Spielberg'
        """
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results)
        df.columns = ['film']
        print(df)
        #for row in results:   #senza usare pandas
        #print(row)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()


def show_tvma_large_cast(connection):
    try:
        cursor = connection.cursor()
        query = """
        SELECT f.title, f.rating, COUNT(DISTINCT ap.id_actor) AS actors_count
        FROM film f
        JOIN actor_participation ap ON f.show_id = ap.show_id
        WHERE f.rating = 'TV-MA'
        GROUP BY f.show_id
        HAVING actors_count > 6
        ORDER BY actors_count DESC
        """
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results)
        df.columns = ['title', 'rating', 'actors_count']
        print(df)
        #for row in results: #senza usare pandas
        #(row)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()


# da sistemare (inserire try - except e commit - rollback), valutare anche l'uso di execute_list query al posto
# di execute_query
def add_show_to_database(connection):
    # Raccogliere i dati dall'utente
    title = input("Inserisci il titolo del film: ")
    type_show = input("Inserisci il tipo dello show: ")
    country = input("Inserisci il country: ")
    date_added = input("Inserisci la data di aggiunta al catalogo (AAAA-MM-DD): ")
    release_year = input("Inserisci l'anno di uscita: ")
    rating = input("Inserisci la classificazione del film: ")
    duration = input("Inserisci la durata del film: ")
    listed_in = input("Inserisci le categorie (separate da virgola): ")
    description = input("Inserisci la descrizione del film: ")
    directors = input("Inserisci i nomi dei registi separati da virgola: ").split(',')
    actors = input("Inserisci i nomi degli attori separati da virgola: ").split(',')


    # Inserire il film
    movie_query = """
    INSERT INTO film (title, type, country, date_added, release_year, rating, duration, listed_in, description)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    execute_query(connection, movie_query, (title, type_show, country, date_added, release_year, rating, duration, listed_in, description))
    movie_id = get_last_inserted_id(connection)

    # Inserire i registi e aggiornare la tabella di partecipazione
    director_ids = []
    for director in directors:
        director = director.strip()
        director_query = "INSERT INTO directors (name) VALUES (%s);"
        execute_query(connection, director_query, (director,))
        director_id = get_last_inserted_id(connection)
        director_ids.append(director_id)
        # Aggiornare la tabella director_participation
        part_query = "INSERT INTO director_participation (show_id, id_director) VALUES (%s, %s);"
        execute_query(connection, part_query, (movie_id, director_id))

    # Inserire gli attori e aggiornare la tabella di partecipazione
    actor_ids = []
    for actor in actors:
        actor = actor.strip()
        actor_query = "INSERT INTO actors (name) VALUES (%s);"
        execute_query(connection, actor_query, (actor,))
        actor_id = get_last_inserted_id(connection)
        actor_ids.append(actor_id)
        # Aggiornare la tabella actor_participation
        part_query = "INSERT INTO actor_participation (show_id, id_actor) VALUES (%s, %s);"
        execute_query(connection, part_query, (movie_id, actor_id))


def get_last_inserted_id(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT LAST_INSERT_ID();")
    last_id = cursor.fetchone()[0]
    cursor.close()
    return last_id
