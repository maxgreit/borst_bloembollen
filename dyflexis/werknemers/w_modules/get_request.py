from w_modules.log import log
import pandas as pd
import requests
import logging

def get_request(greit_connection_string, klant, bron, script, script_id, tabelnaam, base_url, endpoint, token, system_name):
    total_rows = 0
    
    # Logging
    print(f"Start GET Requests")
    log(greit_connection_string, klant, bron, f"Start GET Requests", script, script_id, tabelnaam)

    page = 1
    all_data = []
    page_count = float('inf')

    while page <= page_count:
        # Define the full URL and endpoint
        url = base_url + system_name
        extension = f"?page={page}"
        full_url = url + endpoint + extension

        headers = {
            "Authorization": "Token " + token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        # Get request with full_url and endpoint
        try:
            response = requests.get(full_url, headers=headers)
        except Exception as e:
            print(f"FOUTMELDING | GET Request mislukt: {e}")
            log(greit_connection_string, klant, bron, f"FOUTMELDING | GET Request mislukt: {e}", script, script_id, tabelnaam)
            return None

        # Check if request was successful
        if response.status_code == 200:
            # Turn response into JSON data
            data = response.json()

            if tabelnaam == 'Werknemers':
                # Extraheer geregistreerde uren uit data
                shifts = data['employeeData']
            
                # Append shifts to all_data
                all_data.extend(shifts)

                # Length of all_data
                total_rows = len(all_data)

                # Update page_count with page if not initialized, and make page_count and integer
                if page == 1:
                    page_count = data['page']
                    page_count = int(page_count)

                # Print progressie
                print(f"Huidige pagina: {page} | Totaal aantal rijen: {total_rows}")

                # Increment page number
                page += 1

        else:
            print(f"Error: {response.status_code} - {response.text}")
            log(greit_connection_string, klant, bron, f"FOUTMELDING | Uitvoer GET Request mislukt: {response.status_code} - {response.text}", script, script_id, tabelnaam)
            break  # Exit loop on error

    # Create DataFrame from all_data
    df = pd.DataFrame(all_data)

    return df

def execute_get_request(greit_connection_string, base_url, token, system_name, tabelnaam, endpoint, klant, bron, script, script_id):
    try:
        df = get_request(greit_connection_string, klant, bron, script, script_id, tabelnaam, base_url, endpoint, token, system_name)
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