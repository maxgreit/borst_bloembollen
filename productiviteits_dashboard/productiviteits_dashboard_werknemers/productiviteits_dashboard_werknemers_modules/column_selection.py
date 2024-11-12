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