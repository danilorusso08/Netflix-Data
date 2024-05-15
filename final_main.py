from function_create_tables_queries import *
from function_insert_data_from_csv import *


def main_menu():
    DB_NAME = None
    connection = None

    while True:
        print("\nMenu:")
        print("1. Visualizza attori che hanno partecipato a più show (i primi 10)")
        print("2. Show che hanno come regista 'Steven Spielberg'")
        print("3. Show con un determinato rating 'TV-MA' che hanno più di 6 componenti nel cast")
        print("4. Aggiungi show")
        print("5. Elimina show")
        print("6. Connetti o crea database")
        print("7. Crea tabelle (solo prima esecuzione)")
        print("8. Inserisci dati dal csv (solo prima esecuzione)")
        print("0. Esci")

        choice = input("Scelta: ")

        if choice == '1':
            if connection:
                show_top_actors(connection)
            else:
                print("Devi prima connetterti a un database.")
        elif choice == '2':
            if connection:
                show_spielberg_shows(connection)
            else:
                print("Devi prima connetterti a un database.")
        elif choice == '3':
            if connection:
                show_tvma_large_cast(connection)
            else:
                print("Devi prima connetterti a un database.")
        elif choice == '4':
            if connection:
                add_show_to_database(connection)
            else:
                print("Devi prima connetterti a un database.")
        elif choice == '5':
            if connection:
                delete_show(connection)
            else:
                print("Devi prima connetterti a un database.")
        elif choice == '6':
            db_choice = input("Vuoi connetterti a un database esistente? (Si o No): ").strip().lower()
            if db_choice == 'si':
                DB_NAME = input("Inserisci il nome del database esistente: ").strip()
            else:
                DB_NAME = input("Inserisci il nome del nuovo database da creare: ").strip()
                create_db(DB_NAME)
            connection = create_db_connection('localhost', 'root', '', DB_NAME)
            if connection:
                print(f"Connessione al database {DB_NAME} riuscita.")
            else:
                print("Impossibile connettersi o creare il database.")
        elif choice == '7':
            if connection:
                create_tables(connection)
            else:
                print("Devi prima connetterti a un database.")
        elif choice == '8':
            if connection:
                insert_data_from_csv(connection)
            else:
                print("Devi prima connetterti a un database.")
        elif choice == '0':
            if connection:
                connection.close()
            print("Uscita dal programma.")
            break
        else:
            print("Scelta non valida. Riprova.")


main_menu()
