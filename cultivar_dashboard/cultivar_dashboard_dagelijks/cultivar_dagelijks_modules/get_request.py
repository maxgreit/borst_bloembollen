import requests
import pandas as pd
from cultivar_dagelijks_modules.log import log

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