from functions_db import *
import pandas as pd


def insert_data_from_csv(connection):
    try:
        df_netflix = pd.read_csv('netflix_data.csv')
        df_netflix['date_added'] = df_netflix['date_added'].str.strip()
        df_netflix['date_added'] = pd.to_datetime(df_netflix['date_added'], format='%B %d, %Y')
        df_netflix['show_id'] = df_netflix['show_id'].str.replace('s', '', case=False).astype(int)
        # sostituisco i Nan con None perché i Nan mi davano problemi con Mysql
        df_netflix = df_netflix.where(pd.notna(df_netflix), None)

        netflix_clean_directors = []

        for i, row in df_netflix.iterrows():
            if pd.isna(row['director']):
                netflix_clean_directors.append(row.copy())
                continue
            directors = row['director'].split(', ')
            for director in directors:
                new_row = row.copy()
                new_row['director'] = director
                netflix_clean_directors.append(new_row)

        df_netflix_clean = pd.DataFrame(netflix_clean_directors)
        # elimino none per verificare soluzione francesco
        df_netflix_clean = df_netflix_clean.dropna(subset=['director'])
        df_directors = df_netflix_clean[['director']].drop_duplicates()

        netflix_cast_clean = []

        for i, row in df_netflix.iterrows():
            if pd.isna(row['cast']):
                netflix_cast_clean.append(row.copy())
                continue
            cast_members = row['cast'].split(', ')
            for member in cast_members:
                new_row = row.copy()
                new_row['cast'] = member
                netflix_cast_clean.append(new_row)

        df_netflix_clean2 = pd.DataFrame(netflix_cast_clean)
        # elimino none per verificare soluzione francesco
        df_netflix_clean2 = df_netflix_clean2.dropna(subset=['cast'])
        df_cast = df_netflix_clean2[['cast']].drop_duplicates()

        df_directors.reset_index(drop=True, inplace=True)
        df_directors['director_id'] = df_directors.index + 1
        df_cast.reset_index(drop=True, inplace=True)
        df_cast['actor_id'] = df_cast.index + 1

        # df per tabelle molti a molti
        final_df_netflix_cast = df_netflix_clean2.merge(df_cast[['cast', 'actor_id']], on='cast', how='left')
        final_df_netflix_directors = df_netflix_clean.merge(df_directors[['director', 'director_id']], on='director',
                                                            how='left')

        # inserimento dati

        list_tuple_films = [(row['show_id'], row['title'], row['type'], row['country'], row['date_added'],
                             row['release_year'], row['rating'], row['duration'], row['listed_in'], row['description'])
                            for
                            index, row in df_netflix.iterrows()]

        list_tuple_directors = [(row['director_id'], row['director']) for index, row in df_directors.iterrows()]

        list_tuple_actors = [(row['actor_id'], row['cast']) for index, row in df_cast.iterrows()]

        insert_film_query = """
        INSERT INTO film (show_id, title, type, country, date_added, release_year, rating, duration, listed_in, description)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        insert_directors_query = """
        INSERT INTO directors (id, name) VALUES (%s, %s)
        """

        insert_actors_query = """
        INSERT INTO actors (id, name) VALUES (%s, %s)
        """

        list_tuple_director_participation = [(row['show_id'], row['director_id']) for index, row in
                                             final_df_netflix_directors.iterrows()]

        insert_director_participation_query = """
        INSERT INTO director_participation (show_id, id_director) VALUES (%s, %s)
        """
        list_tuple_actor_participation = [(row['show_id'], row['actor_id']) for index, row in
                                          final_df_netflix_cast.iterrows()]

        insert_actor_participation_query = """
        INSERT INTO actor_participation (show_id, id_actor) VALUES (%s, %s)
        """

        # Esecuzione delle query di inserimento
        execute_list_query(connection, insert_film_query, list_tuple_films)
        execute_list_query(connection, insert_directors_query, list_tuple_directors)
        execute_list_query(connection, insert_actors_query, list_tuple_actors)
        execute_list_query(connection, insert_director_participation_query, list_tuple_director_participation)
        execute_list_query(connection, insert_actor_participation_query, list_tuple_actor_participation)

        connection.commit()
        print("Dati inseriti correttamente nel database.")
    except Exception as e:
        print(f"Errore nell'inserimento dei dati: {e}")
