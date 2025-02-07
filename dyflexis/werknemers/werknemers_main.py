from w_modules.config import determine_script_id, create_connection_dict, create_config_dict, retrieve_token_url_system
from w_modules.column_selection import apply_column_selection, alter_columns
from w_modules.get_request import execute_get_request
from w_modules.database import empty_and_fill_table
from w_modules.type_mapping import apply_conversion
from w_modules.table_mapping import apply_mapping
from w_modules.log import end_log, setup_logging
from w_modules.env_tool import env_check
import logging
import time
import os

def main():

    # Lokaal of productieomgeving bepaling
    env_check()

    # Script configuratie
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    klant = "Borst"
    script = "Werknemers"
    bron = 'Dyflexis'
    start_time = time.time()

    # Verbindingsinstellingen
    server = os.getenv('SERVER')
    database = os.getenv('DATABASE')
    username = os.getenv('GEBRUIKERSNAAM')
    password = os.getenv('PASSWORD')
    driver = '{ODBC Driver 18 for SQL Server}'
    greit_connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=no;TrustServerCertificate=no;Connection Timeout=30;'

    # Script ID bepalen
    script_id = determine_script_id(greit_connection_string)
    
    # Set up logging (met database logging)
    setup_logging(greit_connection_string, klant, bron, script, script_id)

    # Connectie dictionary maken
    connection_dict = create_connection_dict(greit_connection_string)
    
    try:
        for klantnaam, (klant_connection_string, type) in connection_dict.items():
            if klantnaam == "Borst Bloembollen":

                # Configuratie dictionary maken
                configuratie_dict = create_config_dict(klant_connection_string)

                for bron, dict in configuratie_dict.items():
                    if bron == 'Dyflexis | Azure SQL Database':
                        # Bron herstel
                        bron = 'Dyflexis'
                        
                        # Token en URL ophalen
                        token, base_url, system_name = retrieve_token_url_system(dict)

                        # Endpoints
                        endpoints = {
                            "Werknemers": "/api2/employee-data",
                        }

                        for tabelnaam, endpoint in endpoints.items():

                            # Data extractie
                            df = execute_get_request(base_url, token, system_name, tabelnaam, endpoint)

                            # Kolommen type conversie
                            converted_df = apply_conversion(df, tabelnaam, greit_connection_string, klant, bron, script, script_id)
                            
                            # Data transformatie
                            transformed_df = apply_mapping(converted_df, tabelnaam)
                            
                            # Kolom selectie
                            selected_df = apply_column_selection(transformed_df, tabelnaam)

                            # Duplicaten en None verwijderen
                            df = alter_columns(selected_df)

                            # Data overdracht
                            empty_and_fill_table(df, tabelnaam, klant_connection_string)

    except Exception as e:
        logging.error(f"Script mislukt: {e}")

    # Eindtijd logging
    end_log(start_time)

if __name__ == "__main__":
    main()