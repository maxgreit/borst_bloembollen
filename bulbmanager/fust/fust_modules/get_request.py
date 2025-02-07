import pandas as pd
import requests
import logging

def get_request(base_url, extensie, token, tabelnaam, teeltjaar):
    # Logging
    logging.info(f"Start tabel {tabelnaam} voor teeltjaar {teeltjaar}")

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
        logging.error(f"GET Request mislukt: {e}")
        return None

    # Data response
    data = response.json()
    df = pd.DataFrame.from_dict(data)

    return df

def execute_get_request(base_url, extensie, token, tabelnaam, teeltjaar):
    try:
        df = get_request(base_url, extensie, token, tabelnaam, teeltjaar)
    except Exception as e:
        logging.error(f"Uitvoer GET Request mislukt: {e}")
    
    # Dataframe check
    if df is None:
        logging.error(f"Geen DataFrame geretourneerd")
        return None
    
    if df.empty:
        logging.error(f"DataFrame is leeg")
        return df
    
    else:
        return df