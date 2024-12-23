from bm_modules.config import create_config_dict, determine_script_id, create_connection_dict, retrieve_token_url
from bm_modules.year_determination import recent_years, past_years
from bm_modules.get_request import execute_get_request
from bm_modules.database import empty_and_fill_table
from bm_modules.type_mapping import apply_conversion
from bm_modules.table_mapping import apply_mapping
from bm_modules.env_tool import env_check
from bm_modules.log import log, end_log
import logging
import time
import os

def main(teeltjaren):

    # Lokaal of productieomgeving bepaling
    env_check()

    # Script configuratie
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    klant = "Borst Bloembollen"
    script = "Bulbmanager | Dagelijks"
    bron = 'Python'
    start_time = time.time()

    # Omgevingsvariabelen
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
                    if bron == 'Bulbmanager | Azure SQL Database':

                        # Token en URL ophalen
                        token, base_url = retrieve_token_url(dict)

                        for teeltjaar in teeltjaren:
                            
                            # Endpoints
                            endpoints = {
                                'Partij_maat': 'partij_maat_details',
                                'Fust': 'fust_details'
                            }

                            for tabelnaam, extensie in endpoints.items():
                                
                                # Data extractie
                                df = execute_get_request(greit_connection_string, base_url, extensie, token, tabelnaam, teeltjaar, klant, bron, script, script_id)
                                
                                # Data transformatie
                                transformed_df = apply_mapping(df, tabelnaam, greit_connection_string, klant, bron, script, script_id)
                                
                                # Kolommen type conversie
                                converted_df = apply_conversion(transformed_df, tabelnaam, greit_connection_string, klant, bron, script, script_id)
                                
                                # Data overdracht
                                empty_and_fill_table(converted_df, tabelnaam, klant_connection_string, greit_connection_string, klant, bron, script, script_id, teeltjaar)

    except Exception as e:
        print(f"FOUTMELDING | Script mislukt: {e}")
        log(greit_connection_string, klant, bron, f"FOUTMELDING | Script mislukt: {e}", script, script_id, tabelnaam)

    # Eindtijd logging
    end_log(start_time, greit_connection_string, klant, bron, script, script_id)

if __name__ == "__main__":
    teeltjaren = past_years()
    
    main(teeltjaren)
    
    teeltjaren = recent_years()
    
    main(teeltjaren)