from pm_modules.config import create_config_dict, determine_script_id, create_connection_dict, retrieve_token_url
from pm_modules.year_determination import recent_years, past_years
from pm_modules.get_request import execute_get_request
from pm_modules.database import empty_and_fill_table
from pm_modules.type_mapping import apply_conversion
from pm_modules.table_mapping import apply_mapping
from pm_modules.log import end_log, setup_logging
from pm_modules.env_tool import env_check
import logging
import time
import os

def main():

    # Lokaal of productieomgeving bepaling
    env_check()

    # Script configuratie
    klant = "Borst"
    script = "Partij Maat | Recent"
    bron = 'Bulbmanager'
    start_time = time.time()
    teeltjaren = recent_years()

    # Omgevingsvariabelen
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
                    if bron == 'Bulbmanager | Azure SQL Database':
                        # Bron herstel
                        bron = "Bulbmanager"

                        # Token en URL ophalen
                        token, base_url = retrieve_token_url(dict)

                        for teeltjaar in teeltjaren:
                            
                            # Endpoints
                            endpoints = {
                                'Partij_maat': 'partij_maat_details',
                            }

                            for tabelnaam, extensie in endpoints.items():
                                
                                # Data extractie
                                df = execute_get_request(base_url, extensie, token, tabelnaam, teeltjaar)
                                
                                # Data transformatie
                                transformed_df = apply_mapping(df, tabelnaam)
                                
                                # Kolommen type conversie
                                converted_df = apply_conversion(transformed_df, tabelnaam)
                                
                                # Data overdracht
                                empty_and_fill_table(converted_df, tabelnaam, klant_connection_string, teeltjaar)

    except Exception as e:
        logging.error(f"Script mislukt: {e}")

    # Eindtijd logging
    end_log(start_time)

if __name__ == "__main__":    
    main()