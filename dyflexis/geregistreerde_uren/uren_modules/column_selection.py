from uren_modules.log import log
import pandas as pd

geregistreerde_uren_columns = [
    "ID",
    "Urenregel_ID",
    "Werknemer_ID",
    "Datum",
    "Jaar",
    "Begin_datum",
    "Eind_datum",
    "Uur_type",
    "Uren",
    "Duur_in_minuten",
    "Duur_in_uren",
    "Pauze_in_minuten",
    "Pauze_in_uren",
    "Status"
]

def select_columns(df, columns):
    # Controleer of de DataFrame leeg is
    if df.empty:
        # Retourneer een melding en None
        print("De DataFrame is leeg. Retourneer een lege DataFrame met de juiste kolommen.")
        return None

    # Selecteer de gewenste kolommen
    df = df[columns]

    return df

def apply_column_selection(df, greit_connection_string, tabelnaam, klant, bron, script, script_id):

    column_selection = {
        'Geregistreerde_uren': geregistreerde_uren_columns,
    }

    # Tabel selectie
    for selection_table, selection in column_selection.items():
        if tabelnaam == selection_table:

            # Kolommen selecteren
            try:
                df = select_columns(df, selection)
                print(f"Kolommen getransformeerd")
                log(greit_connection_string, klant, bron, f"Juiste kolommen geselecteerd", script, script_id, tabelnaam)
            except Exception as e:
                print(f"FOUTMELDING | Kolommen selecteren mislukt: {e}")
                log(greit_connection_string, klant, bron, f"FOUTMELDING | Kolommen selecteren mislukt: {e}", script, script_id, tabelnaam)
    
    return df

def add_calculated_columns(df):
    df['Duur_in_uren'] = df['duration'] / 60
    df['Pauze_in_uren'] = df['breakMinutes'] / 60
    df['Datum'] = pd.to_datetime(df['startDateTime'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['Jaar'] = pd.to_datetime(df['startDateTime'], errors='coerce').dt.year
    
    return df

def alter_columns(df):
    # Replace 'None'
    df.replace('None', None, inplace=True)
    
    # Duplicaten verwijderen op de primary key 'Urenregel_ID'
    df = df.drop_duplicates(subset=['Urenregel_ID'], keep='last')
    
    return df