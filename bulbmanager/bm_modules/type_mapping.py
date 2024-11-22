import pandas as pd
from decimal import Decimal
import numpy as np
from bm_modules.log import log
from pandas.errors import OutOfBoundsDatetime

fust_typing = {
    "Partij": "nvarchar",
    "Partij_afkorting": "nvarchar",
    "Partij_aanduiding": "nvarchar",
    "Kweker": "nvarchar",
    "Klant": "nvarchar",
    "Volgorde": "int",
    "Teeltjaar": "int",
    "Gewas": "nvarchar",
    "Gesorteerd": "bit",
    "Geteld": "bit",
    "Maat": "nvarchar",
    "Aantal_in_kuubskist": "decimal",
    "Aantal_per_meter": "decimal",
    "Cultivar": "nvarchar",
    "Cel": "nvarchar",
    "Vak": "nvarchar",
    "Plantgoed": "bit",
    "Leverbaar": "bit",
    "Uitval_geschat": "bit",
    "Rooi_datum": "date",
    "Rooi_datumtijd": "datetime",
    "Ontvangst_datum": "date",
    "Ontvangst_datumtijd": "datetime",
    "Pel_datum_gepland": "date",
    "Pel_datum_werkelijk": "date",
    "Pel_datumtijd": "datetime",
    "Gespoeld": "bit",
    "Spoel_datum_werkelijk": "date",
    "Gezeefd": "bit",
    "Gezeefd_datum": "date",
    "Uitgezocht": "bit",
    "Uitgezocht_datum": "date",
    "Spoel_datumtijd": "datetime",
    "Droog_datumtijd_start": "datetime",
    "Droog_datumtijd_eind": "datetime",
    "Afgehandeld_datum": "date",
    "Afgehandeld_datumtijd": "datetime",
    "Stoom_datumtijd_start": "datetime",
    "Stoom_datumtijd_eind": "datetime",
    "Fust_volgnummer": "int",
    "Fust_volgnummer_partij": "int",
    "KG_per_fust": "decimal",
    "Aantal_fust": "decimal",
    "Aantal_per_fust": "decimal",
    "Spoelen": "bit",
    "Opmerkingen": "nvarchar",
    "Spoel_tijd": "time",
    "Leeg": "bit",
    "Afgehandeld": "bit",
    "Datumtijd": "datetime",
    "Achternaam": "nvarchar",
    "Minus_7": "bit",
    "Partijnummer": "int",
    "Partij_volgnummer": "int",
    "Fustnummer": "int",
    "Pellen": "bit",
    "Gepeld": "bit",
    "Inactief": "bit",
    "Referentie_maat": "nvarchar",
    "Uit_maat": "nvarchar",
    "Pand": "nvarchar",
    "Perceel": "nvarchar",
    "Gewas_groep": "nvarchar",
    "Certificaat": "nvarchar",
    "Aanmaak_locatie": "nvarchar",
    "Totaal_bollen": "decimal",
    "Restant_bollen": "decimal"
}

partij_maat_typing = {
    "Cultivar": "nvarchar",
    "Vader": "nvarchar",
    "Moeder": "nvarchar",
    "Kweekaanduiding": "nvarchar",
    "Indeling": "nvarchar",
    "Afkorting": "nvarchar",
    "Kwekersrecht": "bit",
    "Kwekers_rechthouder": "nvarchar",
    "Kosten_RR_per_jaar": "decimal",
    "Kleur": "nvarchar",
    "Volgorde_broeien": "nvarchar",
    "Broeisnelheid": "decimal",
    "Beschrijving": "nvarchar",
    "Preparatie_tussen_temp": "nvarchar",
    "Preparatie_koude_weken": "nvarchar",
    "Kas_dagen": "decimal",
    "Beste_broei_periode": "nvarchar",
    "Beschikbaarheid": "nvarchar",
    "Areaal": "nvarchar",
    "Huid_kwaliteit": "decimal",
    "Verhouding": "decimal",
    "Periode": "nvarchar",
    "Zuur_gevoeligheid": "decimal",
    "Kern_rot_gevoeligheid": "nvarchar",
    "Warmte": "bit",
    "Ploidy_niveau": "nvarchar",
    "Aanbod_KG": "decimal",
    "Prijs_per_KG": "decimal",
    "WBSO": "bit",
    "Start_datum_WBSO": "date",
    "Eind_datum_WBSO": "date",
    "Start_oogstjaar_WBSO": "decimal",
    "Eind_oogstjaar_WBSO": "decimal",
    "Selectienummer": "nvarchar",
    "Tentoonstellingsnummer": "nvarchar",
    "Doelmarkt": "nvarchar",
    "Selectie_type": "nvarchar",
    "Jaar_selectie": "nvarchar",
    "Bloei_periode": "nvarchar",
    "Poot_lengte": "nvarchar",
    "Rooi_periode": "nvarchar",
    "Ideale_koud_behoefte": "nvarchar",
    "Bewaar_temperatuur_show": "nvarchar",
    "Plant_methode": "nvarchar",
    "Bijzonderheden": "int",
    "Uitgelicht_bloemen": "bit",
    "Uitgelicht_bollen": "bit",
    "Bloembollen_aanbod": "bit",
    "Plantgoed_aanbod": "bit",
    "Actief_plantgoed": "bit",
    "Categorie": "nvarchar",
    "Inactief": "bit",
    "Kleur_ID": "decimal",
    "Klasse": "nvarchar",
    "Verhandelen": "bit",
    "Origineel": "nvarchar",
    "Lijn_kleur_polygoon": "nvarchar",
    "Oogstmoment": "nvarchar",
    "Vorige_cultivar": "nvarchar",
    "Print": "bit",
    "Jaar_einde_soort": "nvarchar",
    "Koken": "bit",
    "Plant_timeout": "int",
    "Verkoop_op_kleur": "bit",
    "Cultivar_afkorting": "nvarchar",
    "Deadline_pellen": "int",
    "Teeltjaar": "int",
    "Partij": "nvarchar",
    "Certificaat": "int",
    "Totaal_RR_plantgoed": "decimal",
    "Totaal_RR_leverbaar": "decimal",
    "Maat": "nvarchar",
    "Referentie_maat": "nvarchar",
    "Cultivar_maat_totaal_bollen_verkocht": "int"
  }

def convert_column_types(df, column_types):
    pd.set_option('future.no_silent_downcasting', True)

    for column, dtype in column_types.items():
        if column in df.columns:
            try:
                # Vervang None-waarden met een standaardwaarde voordat je de conversie uitvoert
                if dtype == 'int':
                    # Zet niet-numerieke waarden om naar NaN en vul None in met 0
                    df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(int)
                elif dtype == 'nvarchar':
                    df[column] = df[column].fillna('').astype(str)  # Vervang None door lege string
                elif dtype == 'decimal':
                    df[column] = df[column].apply(lambda x: round(Decimal(x), 2) if pd.notna(x) else None)
                elif dtype == 'bit':
                    df[column] = df[column].apply(lambda x: bool(x) if x in [0, 1] else x == -1).fillna(False)
                elif dtype == 'date':
                    df[column] = pd.to_datetime(df[column], errors='coerce').dt.date
                    df[column] = df[column].fillna(pd.NaT)
                elif dtype == 'time':
                    df[column] = pd.to_datetime(df[column], errors='coerce').dt.time
                    df[column] = df[column].fillna(pd.NaT)
                elif dtype == 'datetime':
                    # Zet de kolom om naar datetime
                    df[column] = pd.to_datetime(df[column], errors='coerce')
                    
                    # Controleer of er datums buiten het bereik van SQL Server 'datetime' vallen (1753-9999)
                    if df[column].isna().sum() > 0:  # Foute datums worden vervangen door NaT
                        df[column] = df[column].fillna(pd.NaT)
                    
                    # Zet de datums buiten het bereik van SQL Server 'datetime' (1753-9999) op NaT
                    # SQL Server's datetime heeft een bereik van 1753-9999
                    min_date = pd.Timestamp('1753-01-01')
                    max_date = pd.Timestamp('9999-12-31')
                    df[column] = df[column].apply(lambda x: x if pd.isna(x) or (min_date <= x <= max_date) else pd.NaT)

                else:
                    raise ValueError(f"Onbekend datatype '{dtype}' voor kolom '{column}'.")
            except ValueError as e:
                raise ValueError(f"Fout bij het omzetten van kolom '{column}' naar type '{dtype}': {e}")
            except OutOfBoundsDatetime:
                # Als er een 'OutOfBoundsDatetime' fout is (zoals een datum buiten het bereik van SQL Server),
                # zet dan die waarde op NaT.
                df[column] = pd.NaT
        else:
            raise ValueError(f"Kolom '{column}' niet gevonden in DataFrame.")
    
    return df

def apply_conversion(df, tabelnaam, greit_connection_string, klant, bron, script, script_id):
    column_typing = {
        'Partij_maat': partij_maat_typing,
        'Fust': fust_typing
    }

    # Update typing van kolommen
    for typing_table, typing in column_typing.items():
        if tabelnaam == typing_table:
            
            # Type conversie
            try:
                converted_df = convert_column_types(df, typing)
                print(f"Kolommen type conversie")
                log(greit_connection_string, klant, bron, f"Kolommen type conversie correct uitgevoerd", script, script_id, tabelnaam)
                
                return converted_df
            except Exception as e:
                print(f"FOUTMELDING | Kolommen type conversie mislukt: {e}")
                log(greit_connection_string, klant, bron, f"FOUTMELDING | Kolommen type conversie mislukt: {e}", script, script_id, tabelnaam)
                
            