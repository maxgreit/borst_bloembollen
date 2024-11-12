
import requests
import pandas as pd
from productiviteits_dashboard_geregistreerde_uren_maandelijks_modules.log import log

def get_request(azure_connectie_string, klant, bron, script, script_id, tabelnaam, start_date, end_date, base_url, endpoint, token, system_name):
    total_rows = 0
    
    # Logging
    print(f"Start GET Requests")
    log(azure_connectie_string, klant, bron, f"Start GET Requests", script, script_id, tabelnaam)

    page = 1
    all_data = []
    page_count = float('inf')

    while page <= page_count:
        # Define the full URL and endpoint
        url = base_url + system_name
        extension = f"?startDate={start_date}&endDate={end_date}&page={page}"
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
            log(azure_connectie_string, klant, bron, f"FOUTMELDING | GET Request mislukt: {e}", script, script_id, tabelnaam)
            return None

        # Check if request was successful
        if response.status_code == 200:
            # Turn response into JSON data
            data = response.json()

            if tabelnaam == 'Geplande_uren':
                # Extraheer geplande uren uit data
                shifts = data['shifts']

                # Append shifts to all_data
                all_data.extend(shifts)

                # Length of all_data
                total_rows = len(all_data)

                # Update page_count with pageCount if not initialized
                if page == 1:
                    page_count = data['pageCount']

                # Print progressie
                print(f"Huidige pagina: {page} | Totaal aantal rijen: {total_rows}")

                # Increment page number
                page += 1

            elif tabelnaam == 'Geregistreerde_uren':
                # Extraheer geregistreerde uren uit data
                shifts = data['registeredHours']
            
                # Append shifts to all_data
                all_data.extend(shifts)

                # Length of all_data
                total_rows = len(all_data)

                # Update page_count with pageCount if not initialized
                if page == 1:
                    page_count = data['pageCount']

                # Print progressie
                print(f"Huidige pagina: {page} | Totaal aantal rijen: {total_rows}")

                # Increment page number
                page += 1

        else:
            print(f"Error: {response.status_code} - {response.text}")
            log(azure_connectie_string, klant, bron, f"FOUTMELDING | Uitvoer GET Request mislukt: {response.status_code} - {response.text}", script, script_id, tabelnaam)
            break  # Exit loop on error

    # Create DataFrame from all_data
    df = pd.DataFrame(all_data)

    return df