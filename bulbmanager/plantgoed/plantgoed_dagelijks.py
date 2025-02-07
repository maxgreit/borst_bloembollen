from plant_modules.config import create_config_dict, determine_script_id, create_connection_dict, retrieve_variables
from plant_modules.excel_processing import excel_to_dataframe, delete_excel_file
from plant_modules.execute_selenium import download_plantgoed_file
from plant_modules.year_determination import recent_years, past_years
from plant_modules.database import empty_and_fill_table
from plant_modules.type_mapping import apply_conversion
from plant_modules.table_mapping import apply_mapping
from plant_modules.log import end_log, setup_logging
from plant_modules.env_tool import env_check
import logging
import time
import os

def main():

    # Lokaal of productieomgeving bepaling
    env_check()

    # Script configuratie
    klant = "Borst"
    script = "Plantgoed | Recent"
    bron = 'Bulbmanager'
    tabelnaam = 'Plantgoed'
    start_time = time.time()
    jaren = [2021]

    # Omgevingsvariabelen
    base_dir = os.getenv("PLANTGOED_BASE_DIR")
    server = os.getenv('SERVER')
    database = os.getenv('DATABASE')
    username = os.getenv('GEBRUIKERSNAAM')
    password = os.getenv('PASSWORD')
    driver = '{ODBC Driver 18 for SQL Server}'
    greit_connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=no;TrustServerCertificate=no;Connection Timeout=30;'

    # Script ID bepalen
    script_id = determine_script_id(greit_connection_string, klant, bron, script)

    # Set up logging (met database logging)
    setup_logging(greit_connection_string, klant, bron, script, script_id)

    # Connectie dictionary maken
    connection_dict = create_connection_dict(greit_connection_string, klant, bron, script, script_id)

    try:
        for klantnaam, (klant_connection_string, type) in connection_dict.items():
            if klantnaam == "Borst Bloembollen":
                
                # Configuratie dictionary maken
                config_dict = create_config_dict(klant_connection_string, greit_connection_string, klant, bron, script, script_id)

                # Retrieve variables
                url, bm_username, bm_password = retrieve_variables(config_dict)
                
                for jaar in jaren:
                    logging.info(f"Begonnen met ophalen data van {jaar}.")
                    
                    # Selenium script aanroepen
                    download_plantgoed_file(url, bm_username, bm_password, base_dir, jaar)
                    
                    # Dataframe ophalen
                    df = excel_to_dataframe(base_dir, jaar)
                    
                    # Jaar kolom toevoegen
                    df['Jaar'] = jaar
                    
                    # Data transformatie
                    transformed_df = apply_mapping(df, tabelnaam, greit_connection_string, klant, bron, script, script_id)
                    
                    # Kolommen type conversie
                    converted_df = apply_conversion(transformed_df, tabelnaam, greit_connection_string, klant, bron, script, script_id)
                    
                    # Data overdracht
                    empty_and_fill_table(converted_df, tabelnaam, klant_connection_string, jaar)
                    
                    # Excel bestand verwijderen
                    delete_excel_file(base_dir, jaar)
                
    except Exception as e:
        logging.error(f"Script mislukt: {e}")

    # Eindtijd logging
    end_log(start_time)

if __name__ == "__main__":    
    main()