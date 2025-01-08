from uren_modules.config import determine_script_id, create_connection_dict, create_config_dict, retrieve_token_url_system
from uren_modules.column_selection import apply_column_selection, add_calculated_columns, alter_columns
from uren_modules.year_determination import this_year, last_year
from uren_modules.get_request import execute_get_request
from uren_modules.database import empty_and_fill_table
from uren_modules.type_mapping import apply_conversion
from uren_modules.table_mapping import apply_mapping
from uren_modules.env_tool import env_check
from uren_modules.log import log, end_log
import time
import os

def main():

    # Lokaal of productieomgeving bepaling
    env_check()

    # Script configuratie
    klant = "Borst Bloembollen"
    script = "Geregistreerde Uren | Huidig Jaar"
    bron = 'Dyflexis'
    start_time = time.time()
    datum_range = this_year()

    # Verbindingsinstellingen
    server = os.getenv('SERVER')
    database = os.getenv('DATABASE')
    username = os.getenv('GEBRUIKERSNAAM')
    password = os.getenv('PASSWORD')
    driver = '{ODBC Driver 18 for SQL Server}'
    greit_connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=no;TrustServerCertificate=no;Connection Timeout=30;'

    # Script ID bepalen
    script_id = determine_script_id(greit_connection_string, klant, bron, script)

    # Connectie dictionary maken
    connection_dict = create_connection_dict(greit_connection_string, klant, bron, script, script_id)
    
    try:
        for klantnaam, (klant_connection_string, type) in connection_dict.items():
            if klantnaam == "Borst Bloembollen":

                # Configuratie dictionary maken
                configuratie_dict = create_config_dict(klant_connection_string, greit_connection_string, klant, bron, script, script_id)

                for bron, dict in configuratie_dict.items():
                    if bron == 'Dyflexis | Azure SQL Database':
                        
                        # Token en URL ophalen
                        token, base_url, system_name = retrieve_token_url_system(dict)
                        
                        # Datum extraheren
                        for jaar, [start_date, end_date] in datum_range.items():
                            
                            # Endpoints
                            endpoints = {
                                "Geregistreerde_uren": "/api/business/v3/registered-hours",
                            }

                            for tabelnaam, endpoint in endpoints.items():

                                # Data extractie
                                df = execute_get_request(greit_connection_string, base_url, token, system_name, tabelnaam, endpoint, start_date, end_date, klant, bron, script, script_id)

                                # Berekende kolommen toevoegen
                                extended_df = add_calculated_columns(df)

                                # Kolommen type conversie
                                converted_df = apply_conversion(extended_df, tabelnaam, greit_connection_string, klant, bron, script, script_id)
                                
                                # Data transformatie
                                transformed_df = apply_mapping(converted_df, tabelnaam, greit_connection_string, klant, bron, script, script_id)
                                
                                # Kolom selectie
                                selected_df = apply_column_selection(transformed_df, greit_connection_string, tabelnaam, klant, bron, script, script_id)

                                # Duplicaten en None verwijderen
                                df = alter_columns(selected_df)

                                # Data overdracht
                                empty_and_fill_table(df, tabelnaam, klant_connection_string, greit_connection_string, klant, bron, script, script_id, jaar)

    except Exception as e:
        print(f"FOUTMELDING | Script mislukt: {e}")
        log(greit_connection_string, klant, bron, f"FOUTMELDING | Script mislukt: {e}", script, script_id)

    # Eindtijd logging
    end_log(start_time, greit_connection_string, klant, bron, script, script_id)

if __name__ == "__main__":
    main()