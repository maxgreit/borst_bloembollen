from decimal import Decimal
import pandas as pd
import logging

werknemers_typing = {
    "dyflexisId": "int",
    "personnelNumber": "int",
    "firstName": "nvarchar",
    "lastNamePrefix": "nvarchar",
    "lastName": "nvarchar",
    "employmentStart": "date",
    "employmentEnd": "date",
}

def convert_column_types(df, column_types):
    pd.set_option('future.no_silent_downcasting', True)
    
    for column, dtype in column_types.items():
        if column in df.columns:
            try:
                if dtype == 'int':
                    # Zet niet-numerieke waarden om naar NaN
                    df[column] = pd.to_numeric(df[column], errors='coerce')
                    invalid_values = df[column].isnull()
                    
                    # Specifieke ongeldige waarden printen
                    if invalid_values.any():
                        ongeldige_waarden = df[column][invalid_values].unique()
                        print(f"Waarschuwing: {len(ongeldige_waarden)} ongeldige waarden gevonden in kolom '{column}': {ongeldige_waarden}, deze worden vervangen door 0.")
                        df[column] = df[column].fillna(0)  # Vervang NaN door 0
                    
                    df[column] = df[column].astype(int)
                elif dtype == 'nvarchar':
                    df[column] = df[column].astype(str)
                elif dtype == 'decimal':
                    df[column] = df[column].apply(lambda x: round(Decimal(x), 2) if pd.notna(x) else None)
                elif dtype == 'bit':
                    df[column] = df[column].apply(lambda x: bool(x) if x in [0, 1] else x == -1)
                elif dtype == 'date':
                    df[column] = pd.to_datetime(df[column], errors='coerce').dt.strftime('%Y-%m-%d')
                elif dtype == 'datetime':
                    df[column] = pd.to_datetime(df[column], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    raise ValueError(f"Onbekend datatype '{dtype}' voor kolom '{column}'.")
            except ValueError as e:
                raise ValueError(f"Fout bij het omzetten van kolom '{column}' naar type '{dtype}': {e}")
        else:
            raise ValueError(f"Kolom '{column}' niet gevonden in DataFrame.")
    
    return df

def apply_conversion(df, tabelnaam, greit_connection_string, klant, bron, script, script_id):
    # Kolom typing
    column_typing = {
        'Werknemers': werknemers_typing,
    }

    # Update typing van kolommen
    for typing_table, typing in column_typing.items():
        if tabelnaam == typing_table:
            
            # Type conversie
            try:
                converted_df = convert_column_types(df, typing)
                logging.info(f"Kolommen type conversie")
                
                return converted_df
            except Exception as e:
                logging.error(f"Kolommen type conversie mislukt: {e}")
                
            