import pandas as pd
import logging
import os

def excel_to_dataframe(base_dir, jaar):
    
    # Creëer de locatie van de Excel
    file_path = base_dir + f"/bulbmanager/plantgoed/bestand/Plantgoed_{jaar}.xlsx"
    
    # Turn excel into a dataframe
    df = pd.read_excel(file_path, sheet_name='Worksheet')
    
    return df

def delete_excel_file(base_dir, jaar):
    # Creëer de locatie van de Excel
    file_path = base_dir + f"/bulbmanager/plantgoed/bestand/Plantgoed_{jaar}.xlsx"
    
    try:
        # Controleer of het bestand bestaat
        if os.path.exists(file_path):
            os.remove(file_path)
            logging("Excel bestand verwijderd")
        else:
            logging.warning("Het bestand bestaat niet of is al verwijderd.")
    except Exception as e:
        logging.error({e})