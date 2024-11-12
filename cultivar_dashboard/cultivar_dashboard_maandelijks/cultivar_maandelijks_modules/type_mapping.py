import pandas as pd
from decimal import Decimal
import numpy as np

partij_maat_typing =   {
    "OmsCultivar": "nvarchar",
    "Kleur": "nvarchar",
    "Beschrijving": "nvarchar",
    "Beschikbaarheid": "nvarchar",
    "ZuurGevoeligheid": "int",
    "KernRotGevoeligheid": "nvarchar",
    "ActiefPlantgoed": "bit",
    "Inactief": "bit",
    "OmsCultivarVorig1": "nvarchar",
    "Tjr": "int",
    "Partij": "nvarchar",
    "TotaalRRLeverbaar": "decimal",
    "Maat": "nvarchar",
    "ReferentieMaat": "nvarchar",
    "CultivarMaatTotaalBollenVerkocht": "int",
}

fust_typing = {
    "OmsPartij": "nvarchar",
    "Tjr": "int",
    "Gesorteerd": "bit",
    "Geteld": "bit",
    "Maat": "nvarchar",
    "AantalInKuubskist": "decimal",
    "AantalPerMeter": "decimal",
    "OmsCultivar": "nvarchar",
    "Plantgoed": "bit",
    "UitvalGeschat": "decimal",
    "RooiDatum": "date",
    "FustVolgnummer": "int",
    "FustVolgnummerPartij": "int",
    "KgPerFust": "decimal",
    "AantalFust": "decimal",
    "AantalPerFust": "decimal",
    "Partijnummer": "int",
    "Fustnummer": "int",
    "Inactief": "bit",
    "ReferentieMaat": "nvarchar",
    "UitMaat": "nvarchar",
    "OmsPerceel": "nvarchar",
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
                    df[column] = pd.to_datetime(df[column], errors='coerce').dt.date
                else:
                    raise ValueError(f"Onbekend datatype '{dtype}' voor kolom '{column}'.")
            except ValueError as e:
                raise ValueError(f"Fout bij het omzetten van kolom '{column}' naar type '{dtype}': {e}")
        else:
            raise ValueError(f"Kolom '{column}' niet gevonden in DataFrame.")
    
    return df