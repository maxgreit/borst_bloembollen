werknemers = {
    "dyflexisId": "Werknemer_ID",
    "personnelNumber": "Personeelsnummer",
    "firstName": "Voornaam",
    "lastNamePrefix": "Tussenvoegsel",
    "lastName": "Achternaam",
    "employmentStart": "Startdatum",
    "employmentEnd": "Einddatum",
}

def transform_columns(df, column_mapping):
    # Controleer of de DataFrame leeg is
    
    if df.empty:
        # Retourneer een melding en None
        print("De DataFrame is leeg. Retourneer een lege DataFrame met de juiste kolommen.")
        return None

    # Hernoem de kolommen
    df = df.rename(columns=column_mapping)

    return df