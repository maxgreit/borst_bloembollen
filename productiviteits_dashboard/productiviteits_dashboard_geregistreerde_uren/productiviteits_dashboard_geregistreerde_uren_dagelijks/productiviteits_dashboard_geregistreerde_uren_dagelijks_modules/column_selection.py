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