import requests
import pandas as pd
import logging
from bm_modules.log import log

def get_request(greit_connection_string, base_url, extensie, token, tabelnaam, teeltjaar, klant, bron, script, script_id):
    # Logging
    print(f"Start tabel {tabelnaam} voor teeltjaar {teeltjaar}")
    log(greit_connection_string, klant, bron, f"Start GET Requests", script, script_id, tabelnaam)

    # Endpoint, url en headers instellen
    endpoint = f'{extensie}?Tjr={teeltjaar}'
    url = base_url + endpoint
    headers = {
    'Authorization': 'Bearer ' + token,
    'accept': 'application/json'
    }

    # GET Request
    try:
        response = requests.get(url, headers=headers)
    except Exception as e:
        print(f"FOUTMELDING | GET Request mislukt: {e}")
        log(greit_connection_string, klant, bron, f"FOUTMELDING | GET Request mislukt: {e}", script, script_id, tabelnaam)
        return None

    # Data response
    data = response.json()
    df = pd.DataFrame.from_dict(data)

    return df

def execute_get_request(greit_connection_string, base_url, extensie, token, tabelnaam, teeltjaar, klant, bron, script, script_id):
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
        return None
    
    if df.empty:
        print(f"FOUTMELDING | DataFrame is leeg")
        log(greit_connection_string, klant, bron, f"FOUTMELDING | DataFrame is leeg", script, script_id, tabelnaam)
        return df
    
    else:
        return df