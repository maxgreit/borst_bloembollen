from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import time
from cultivar_dagelijks_modules.log import log
from cultivar_dagelijks_modules.database import connect_to_database, write_to_database, clear_table
from cultivar_dagelijks_modules.config import fetch_configurations, fetch_script_id, fetch_all_connection_strings
from cultivar_dagelijks_modules.table_mapping import transform_columns, partij_maat_details, fust_details
from cultivar_dagelijks_modules.type_mapping import convert_column_types, partij_maat_typing, fust_typing
from cultivar_dagelijks_modules.column_selection import select_columns, partij_maat_columns, fust_columns
from cultivar_dagelijks_modules.get_request import get_request
import logging 


def main():

    if os.path.exists("/Users/maxrood/werk/greit/klanten/borst/.env"):
        load_dotenv()
        print("Lokaal draaien: .env bestand gevonden en geladen.")
        logging.info("Lokaal draaien: .env bestand gevonden en geladen.")
    else:
        logging.info("Draaien in productieomgeving (Azure): .env bestand niet gevonden.")

    # Logging configuratie
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Definiëren van script
    script = "Cultivar | Dagelijks"
    klant = "Borst Bloembollen"

    # Leg de starttijd vast
    start_time = time.time()

    # Aantal retries instellen
    max_retries = 3
    retry_delay = 5

    # Verbindingsinstellingen
    server = os.getenv('SERVER')
    database = os.getenv('DATABASE')
    username = os.getenv('GEBRUIKERSNAAM')
    password = os.getenv('PASSWORD')
    driver = '{ODBC Driver 18 for SQL Server}'

    # Verbindingsstring
    greit_connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=no;TrustServerCertificate=no;Connection Timeout=30;'

    # ScriptID ophalen
    try:
        database_conn = connect_to_database(greit_connection_string)
    except Exception as e:
        logging.info(f"Verbinding met database mislukt, foutmelding: {e}")
    if database_conn:
        logging.info(f"Verbinding met database geslaagd")
        cursor = database_conn.cursor()
        latest_script_id = fetch_script_id(cursor)
        logging.info(f"ScriptID: {latest_script_id}")
        database_conn.close()

        if latest_script_id:
            script_id = latest_script_id + 1
        else:
            script_id = 1
    
    logging.info(f"ScriptID: {script_id}")

    # Start logging
    bron = 'Python'
    log(greit_connection_string, klant, bron, f"Script gestart", script, script_id)

    # Verbinding maken met database
    try:
        database_conn = connect_to_database(greit_connection_string)
    except Exception as e:
        logging.info(f"Verbinding met database mislukt, foutmelding: {e}")
    if database_conn:
        logging.info(f"Verbinding met database opnieuw geslaagd")
        cursor = database_conn.cursor()
        connection_dict = None
        for attempt in range(max_retries):
            try:
                connection_dict = fetch_all_connection_strings(cursor)
                if connection_dict:
                    break
            except Exception as e:
                time.sleep(retry_delay)
        database_conn.close()
        if connection_dict:

            # Start logging
            log(greit_connection_string, klant, bron, f"Ophalen connectiestrings gestart", script, script_id)
        else:
            # Foutmelding logging
            print(f"FOUTMELDING | Ophalen connectiestrings mislukt na meerdere pogingen")
            log(greit_connection_string, klant, bron, f"FOUTMELDING | Ophalen connectiestrings mislukt na meerdere pogingen", script, script_id)
    else:
        # Foutmelding logging
        print(f"FOUTMELDING | Verbinding met database mislukt na meerdere pogingen")
        log(greit_connection_string, klant, bron, f"FOUTMELDING | Verbinding met database mislukt na meerdere pogingen", script, script_id)

    for klantnaam, (klant_connection_string, type) in connection_dict.items():
        # Skip de klant als type niet gelijk is aan 1
        if type != 1:
            print(f"Skip {klantnaam}")
            log(greit_connection_string, klant, bron, f"Skip {klantnaam}", script, script_id)
            continue

        if klantnaam == "Borst Bloembollen":
            logging.info(f"Start {klantnaam} script")

            # Verbinding maken met database
            try:
                database_conn = connect_to_database(klant_connection_string)
            except Exception as e:
                logging.info(f"Verbinding met database mislukt, foutmelding: {e}")

            if database_conn:
                cursor = database_conn.cursor()

                # Ophalen connection_dict met retries
                configuratie_dict = None
                for attempt in range(max_retries):
                    try:
                        configuratie_dict = fetch_configurations(cursor)
                        if configuratie_dict:
                            break
                    except Exception as e:
                        time.sleep(retry_delay)

                database_conn.close()
            
                if configuratie_dict:
                    # Start logging
                    log(greit_connection_string, klant, bron, f"Ophalen configuratiegegevens gestart", script, script_id)
                else:
                    # Foutmelding logging
                    print(f"FOUTMELDING | Ophalen connectiestrings mislukt na meerdere pogingen")
                    log(greit_connection_string, klant, bron, f"FOUTMELDING | Ophalen configuratiengegevens mislukt na meerdere pogingen", script, script_id)
            else:
                # Foutmelding logging
                print(f"FOUTMELDING | Verbinding met database mislukt na meerdere pogingen")
                log(greit_connection_string, klant, bron, f"FOUTMELDING | Verbinding met database mislukt na meerdere pogingen", script, script_id)

            for bron, dict in configuratie_dict.items():
                if bron == 'Bulbmanager | Azure SQL Database':
                    logging.info(f"Start {bron} script")
                    for configuratie, waarde in dict.items():
                        if configuratie == 'Token':
                            token = waarde
                        elif configuratie == 'Base_url':
                            base_url = waarde
            
                    # Teeltjaren
                    huidig_jaar = datetime.now().year
                    vorig_jaar = huidig_jaar - 1
                    teeltjaren = [vorig_jaar, huidig_jaar]

                    for teeltjaar in teeltjaren:
                        
                        # Endpoints
                        endpoints = {
                            'Partij_maat': 'partij_maat_details',
                            'Fust': 'fust_details'
                        }

                        for tabelnaam, extensie in endpoints.items():
                            
                            # Voer GET Request uit
                            try:
                                df = get_request(greit_connection_string, base_url, extensie, token, tabelnaam, teeltjaar, klant, bron, script, script_id)
                            except Exception as e:
                                print(f"FOUTMELDING | Uitvoer GET Request mislukt: {e}")
                                log(greit_connection_string, klant, bron, f"FOUTMELDING | Uitvoer GET Request mislukt: {e}", script, script_id, tabelnaam)
                                logging.info(f"Verbinding met database mislukt, foutmelding: {e}")
                            # Dataframe check
                            if df is None:
                                print(f"FOUTMELDING | Geen DataFrame geretourneerd")
                                log(greit_connection_string, klant, bron, f"FOUTMELDING | Geen DataFrame geretourneerd", script, script_id, tabelnaam)
                                continue
                            if df.empty:
                                print(f"FOUTMELDING | DataFrame is leeg")
                                log(greit_connection_string, klant, bron, f"FOUTMELDING | DataFrame is leeg", script, script_id, tabelnaam)
                                continue

                            # Kolom typing
                            column_typing = {
                                'Partij_maat': partij_maat_typing,
                                'Fust': fust_typing
                            }

                            # Update typing van kolommen
                            for typing_table, typing in column_typing.items():
                                if tabelnaam == typing_table:
                                    
                                    # Type conversie
                                    try:
                                        df = convert_column_types(df, typing)
                                        print(f"Kolommen type conversie")
                                        log(greit_connection_string, klant, bron, f"Kolommen type conversie correct uitgevoerd", script, script_id, tabelnaam)
                                    except Exception as e:
                                        print(f"FOUTMELDING | Kolommen type conversie mislukt: {e}")
                                        log(greit_connection_string, klant, bron, f"FOUTMELDING | Kolommen type conversie mislukt: {e}", script, script_id, tabelnaam)

                            # Kolom mapping
                            column_mapping = {
                                'Partij_maat': partij_maat_details,
                                'Fust': fust_details
                            }

                            # Tabel mapping
                            for mapping_table, mapping in column_mapping.items():
                                if tabelnaam == mapping_table:

                                    # Transformeer de kolommen
                                    try:
                                        df = transform_columns(df, mapping)
                                        print(f"Kolommen getransformeerd")
                                        log(greit_connection_string, klant, bron, f"Mapping van kolommen correct uitgevoerd", script, script_id, tabelnaam)
                                    except Exception as e:
                                        print(f"FOUTMELDING | Kolommen transformeren mislukt: {e}")
                                        log(greit_connection_string, klant, bron, f"FOUTMELDING | Kolommen transformeren mislukt: {e}", script, script_id, tabelnaam)

                            # Kolom selectie
                            column_selection = {
                                'Partij_maat': partij_maat_columns,
                                'Fust': fust_columns
                            }

                            # Tabel selectie
                            for selection_table, selection in column_selection.items():
                                if tabelnaam == selection_table:

                                    # Kolommen selecteren
                                    try:
                                        df = select_columns(df, selection)
                                        print(f"Kolommen getransformeerd")
                                        log(greit_connection_string, klant, bron, f"Juiste kolommen geselecteerd", script, script_id, tabelnaam)
                                    except Exception as e:
                                        print(f"FOUTMELDING | Kolommen selecteren mislukt: {e}")
                                        log(greit_connection_string, klant, bron, f"FOUTMELDING | Kolommen selecteren mislukt: {e}", script, script_id, tabelnaam)
                            
                            # Tabel leeg maken
                            try:
                                clear_table(klant_connection_string, tabelnaam, teeltjaar)
                                print(f"Tabel {tabelnaam} voor teeltjaar {teeltjaar} leeg gemaakt")
                                log(greit_connection_string, klant, bron, f"Tabel leeg gemaakt voor teeltjaar {teeltjaar}", script, script_id, tabelnaam)
                            except Exception as e:
                                print(f"FOUTMELDING | Tabel leeg maken mislukt: {e}")
                                log(greit_connection_string, klant, bron, f"FOUTMELDING | Tabel leeg maken mislukt voor teeltjaar {teeltjaar}: {e}", script, script_id, tabelnaam)

                            # Tabel vullen
                            try:
                                print(f"Volledige lengte {tabelnaam}: ", len(df))
                                write_to_database(df, tabelnaam, klant_connection_string)
                                print(f"Tabel {tabelnaam} voor teeltjaar {teeltjaar} gevuld")
                                log(greit_connection_string, klant, bron, f"Tabel gevuld met {len(df)} rijen", script, script_id, tabelnaam)
                            except Exception as e:
                                print(f"FOUTMELDING | Tabel vullen mislukt: {e}")
                                log(greit_connection_string, klant, bron, f"FOUTMELDING | Tabel vullen mislukt: {e}", script, script_id, tabelnaam)

    # Eindtijd logging
    bron = 'Python'
    eindtijd = time.time()
    tijdsduur = timedelta(seconds=(eindtijd - start_time))
    tijdsduur_str = str(tijdsduur).split('.')[0]
    log(greit_connection_string, klant, bron, f"Script gestopt in {tijdsduur_str}", script, script_id)
    print(f"Script gestopt in {tijdsduur_str}")

if __name__ == "__main__":
    main()