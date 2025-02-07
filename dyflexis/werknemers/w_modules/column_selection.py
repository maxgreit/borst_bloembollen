import logging

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
        logging.error("De DataFrame is leeg. Retourneer een lege DataFrame met de juiste kolommen.")
        return None

    # Selecteer de gewenste kolommen
    df = df[columns]

    return df

def apply_column_selection(df, tabelnaam):

    column_selection = {
    'Werknemers': werknemers_columns,
    }

    # Tabel selectie
    for selection_table, selection in column_selection.items():
        if tabelnaam == selection_table:

            # Kolommen selecteren
            try:
                df = select_columns(df, selection)
                logging.info(f"Kolommen getransformeerd")
            except Exception as e:
                logging.error(f"Kolommen selecteren mislukt: {e}")
    
    return df

def alter_columns(df):
    # Replace 'None'
    df.replace('None', None, inplace=True)
    
    return df