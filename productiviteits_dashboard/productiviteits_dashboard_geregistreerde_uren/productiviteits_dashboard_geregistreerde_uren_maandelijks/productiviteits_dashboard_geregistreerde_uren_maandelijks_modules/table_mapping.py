geregistreerde_uren = {
    "departmentId": "ID",
    "id": "Urenregel_ID",
    "employeeId": "Werknemer_ID",
    "startDateTime": "Begin_datum",
    "endDateTime": "Eind_datum",
    "hours": "Uren",
    "breakMinutes": "Pauze_in_minuten",
    "duration": "Duur_in_minuten",
    "hourType": "Uur_type",
    "status": "Status"
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