from hour_modules.log import log

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

def apply_mapping(df, tabelnaam, greit_connection_string, klant, bron, script, script_id):
    
    column_mapping = {
    'Geregistreerde_uren': geregistreerde_uren,
    }

    # Tabel mapping
    for mapping_table, mapping in column_mapping.items():
        if tabelnaam == mapping_table:

            # Transformeer de kolommen
            try:
                transformed_df = transform_columns(df, mapping)
                print(f"Kolommen getransformeerd")
                log(greit_connection_string, klant, bron, f"Mapping van kolommen correct uitgevoerd", script, script_id, tabelnaam)
                
                return transformed_df
            except Exception as e:
                print(f"FOUTMELDING | Kolommen transformeren mislukt: {e}")
                log(greit_connection_string, klant, bron, f"FOUTMELDING | Kolommen transformeren mislukt: {e}", script, script_id, tabelnaam)
            
            