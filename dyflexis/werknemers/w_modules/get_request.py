import pandas as pd
import requests
import logging

def get_request(tabelnaam, base_url, endpoint, token, system_name):
    total_rows = 0
    
    # Logging
    logging.info(f"Start GET Requests")

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
            logging.error(f"FOUTMELDING | GET Request mislukt: {e}")
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
            logging.error(f"{response.status_code} - {response.text}")
            break  # Exit loop on error

    # Create DataFrame from all_data
    df = pd.DataFrame(all_data)

    return df

def execute_get_request(base_url, token, system_name, tabelnaam, endpoint):
    try:
        df = get_request(tabelnaam, base_url, endpoint, token, system_name)
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