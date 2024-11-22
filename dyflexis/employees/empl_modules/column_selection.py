import pandas as pd
from empl_modules.log import log

werknemers_columns = [
    "Werknemer_ID",
    "Personeelsnummer",
    "Voornaam",
    "Tussenvoegsel",
    "Achternaam",
    "Startdatum",
    "Einddatum"
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
    'Werknemers': werknemers_columns,
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

def alter_columns(df):
    # Replace 'None'
    df.replace('None', None, inplace=True)
    
    return df