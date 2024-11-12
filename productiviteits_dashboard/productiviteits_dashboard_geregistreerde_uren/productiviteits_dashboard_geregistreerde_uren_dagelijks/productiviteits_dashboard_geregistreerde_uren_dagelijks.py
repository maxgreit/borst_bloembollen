from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import time
from productiviteits_dashboard_geregistreerde_uren_dagelijks_modules.log import log
from productiviteits_dashboard_geregistreerde_uren_dagelijks_modules.database import connect_to_database, write_to_database, clear_table
from productiviteits_dashboard_geregistreerde_uren_dagelijks_modules.config import fetch_configurations, fetch_script_id, fetch_all_connection_strings
from productiviteits_dashboard_geregistreerde_uren_dagelijks_modules.table_mapping import transform_columns, geregistreerde_uren
from productiviteits_dashboard_geregistreerde_uren_dagelijks_modules.type_mapping import convert_column_types, geregistreerde_uren_typing
from productiviteits_dashboard_geregistreerde_uren_dagelijks_modules.column_selection import select_columns, geregistreerde_uren_columns
from productiviteits_dashboard_geregistreerde_uren_dagelijks_modules.get_request import get_request
import pandas as pd
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
    script = "Geregistreerde Uren | Dagelijks"
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
    database_conn = connect_to_database(greit_connection_string)
    if database_conn:
        cursor = database_conn.cursor()
        latest_script_id = fetch_script_id(cursor)
        database_conn.close()

        if latest_script_id:
            script_id = latest_script_id + 1
        else:
            script_id = 1

    # Start logging
    bron = 'Python'
    log(greit_connection_string, klant, bron, f"Script gestart", script, script_id)

    # Verbinding maken met database
    database_conn = connect_to_database(greit_connection_string)
    if database_conn:
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

            # Verbinding maken met database
            database_conn = connect_to_database(klant_connection_string)

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
                if bron == 'Dyflexis | Azure SQL Database':
                    print(f"Bron: {bron}")
                    for configuratie, waarde in dict.items():
                        if configuratie == 'Token':
                            token = waarde
                        elif configuratie == 'Base_url':
                            base_url = waarde
                        elif configuratie == 'System_name':
                            system_name = waarde

                    # Datum range
                    huidig_jaar = datetime.now().year
                    vorig_jaar = huidig_jaar - 1
                    datum_range = {
                        huidig_jaar: [f"{huidig_jaar}-01-01", f"{huidig_jaar}-12-31"],
                        vorig_jaar: [f"{vorig_jaar}-01-01", f"{vorig_jaar}-12-31"],
                    }

                    # Datum extraheren
                    for jaar, [start_date, end_date] in datum_range.items():
                        # Start logging
                        print(f"Start nieuw jaargang: {jaar}")
                        log(greit_connection_string, klant, bron, f"Start nieuw jaargang", script, script_id)

                        # Endpoints
                        endpoints = {
                            "Geregistreerde_uren": "/api/business/v3/registered-hours",
                        }

                        for tabel, endpoint in endpoints.items():
                            # Start logging
                            print(f"Start tabel: {tabel}")
                            log(greit_connection_string, klant, bron, f"Start tabel", script, script_id, tabel)

                            # Request maken
                            df = get_request(greit_connection_string, klant, bron, script, script_id, tabel, start_date, end_date, base_url, endpoint, token, system_name)
                            
                            # Dataframe check
                            if df is None:
                                print(f"FOUTMELDING | Geen DataFrame geretourneerd")
                                log(greit_connection_string, klant, bron, f"FOUTMELDING | Geen DataFrame geretourneerd", script, script_id, tabel)
                                continue
                            if df.empty:
                                print(f"FOUTMELDING | DataFrame is leeg")
                                log(greit_connection_string, klant, bron, f"FOUTMELDING | DataFrame is leeg", script, script_id, tabel)
                                continue

                            # Kolommen toevoegen
                            df['Duur_in_uren'] = df['duration'] / 60
                            df['Pauze_in_uren'] = df['breakMinutes'] / 60
                            df['Datum'] = pd.to_datetime(df['startDateTime'], errors='coerce').dt.strftime('%Y-%m-%d')
                            df['Jaar'] = pd.to_datetime(df['startDateTime'], errors='coerce').dt.year

                            # Kolom typing
                            column_typing = {
                                'Geregistreerde_uren': geregistreerde_uren_typing,
                            }

                            # Update typing van kolommen
                            for typing_table, typing in column_typing.items():
                                if tabel == typing_table:
                                    
                                    # Type conversie
                                    try:
                                        df = convert_column_types(df, typing)
                                        print(f"Kolommen type conversie")
                                        log(greit_connection_string, klant, bron, f"Kolommen type conversie correct uitgevoerd", script, script_id, tabel)
                                    except Exception as e:
                                        print(f"FOUTMELDING | Kolommen type conversie mislukt: {e}")
                                        log(greit_connection_string, klant, bron, f"FOUTMELDING | Kolommen type conversie mislukt: {e}", script, script_id, tabel)

                            # Kolom mapping
                            column_mapping = {
                                'Geregistreerde_uren': geregistreerde_uren,
                            }

                            # Tabel mapping
                            for mapping_table, mapping in column_mapping.items():
                                if tabel == mapping_table:

                                    # Transformeer de kolommen
                                    try:
                                        df = transform_columns(df, mapping)
                                        print(f"Kolommen getransformeerd")
                                        log(greit_connection_string, klant, bron, f"Mapping van kolommen correct uitgevoerd", script, script_id, tabel)
                                    except Exception as e:
                                        print(f"FOUTMELDING | Kolommen transformeren mislukt: {e}")
                                        log(greit_connection_string, klant, bron, f"FOUTMELDING | Kolommen transformeren mislukt: {e}", script, script_id, tabel)

                            # Kolom selectie
                            column_selection = {
                                'Geregistreerde_uren': geregistreerde_uren_columns,
                            }

                            # Tabel selectie
                            for selection_table, selection in column_selection.items():
                                if tabel == selection_table:

                                    # Kolommen selecteren
                                    try:
                                        df = select_columns(df, selection)
                                        print(f"Kolommen getransformeerd")
                                        log(greit_connection_string, klant, bron, f"Juiste kolommen geselecteerd", script, script_id, tabel)
                                    except Exception as e:
                                        print(f"FOUTMELDING | Kolommen selecteren mislukt: {e}")
                                        log(greit_connection_string, klant, bron, f"FOUTMELDING | Kolommen selecteren mislukt: {e}", script, script_id, tabel)

                            # Replace 'None'
                            df.replace('None', None, inplace=True)
                            
                            # Duplicaten verwijderen op de primary key 'Urenregel_ID'
                            df = df.drop_duplicates(subset=['Urenregel_ID'], keep='last')

                            # Tabel leeg maken
                            try:
                                clear_table(klant_connection_string, tabel, jaar)
                                print(f"Tabel {tabel} voor teeltjaar {jaar} leeg gemaakt")
                                log(greit_connection_string, klant, bron, f"Tabel leeg gemaakt", script, script_id, tabel)
                            except Exception as e:
                                print(f"FOUTMELDING | Tabel leeg maken mislukt: {e}")
                                log(greit_connection_string, klant, bron, f"FOUTMELDING | Tabel leeg maken mislukt: {e}", script, script_id, tabel)

                            # Tabel vullen
                            try:
                                print(f"Volledige lengte {tabel}: ", len(df))
                                write_to_database(df, tabel, klant_connection_string)
                                print(f"Tabel {tabel} voor teeltjaar {jaar} gevuld")
                                log(greit_connection_string, klant, bron, f"Tabel gevuld met {len(df)} rijen", script, script_id, tabel)
                            except Exception as e:
                                print(f"FOUTMELDING | Tabel vullen mislukt: {e}")
                                log(greit_connection_string, klant, bron, f"FOUTMELDING | Tabel vullen mislukt: {e}", script, script_id, tabel)


    # Eindtijd logging
    bron = 'Python'
    eindtijd = time.time()
    tijdsduur = timedelta(seconds=(eindtijd - start_time))
    tijdsduur_str = str(tijdsduur).split('.')[0]
    log(greit_connection_string, klant, bron, f"Script gestopt in {tijdsduur_str}", script, script_id)
    print(f"Script gestopt in {tijdsduur_str}")

if __name__ == "__main__":
    main()