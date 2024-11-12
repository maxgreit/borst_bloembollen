partij_maat_details = {
    "OmsCultivar": "Cultivar",
    "Kleur": "Kleur",
    "Beschrijving": "Beschrijving",
    "Beschikbaarheid": "Beschikbaarheid",
    "ZuurGevoeligheid": "Zuur_gevoeligheid",
    "KernRotGevoeligheid": "Kern_rot_gevoeligheid",
    "AanbodKg": "Aanbod_kg",
    "PrijsPerKg": "Prijs_per_kg",
    "ActiefPlantgoed": "Actief_plantgoed",
    "Inactief": "Inactief",
    "Tjr": "Teeltjaar",
    "Partij": "Partij",
    "TotaalRRPlantgoed": "Totaal_RR_plantgoed",
    "TotaalRRLeverbaar": "Totaal_RR_leverbaar",
    "Maat": "Maat",
    "ReferentieMaat": "Referentie_maat",
    "CultivarMaatTotaalBollenVerkocht": "Cultivar_maat_totaal_bollen_verkocht",
}

fust_details = {
    "OmsPartij": "Omschrijving_partij",
    "Tjr": "Teeltjaar",
    "Gesorteerd": "Gesorteerd",
    "Geteld": "Geteld",
    "Maat": "Maat",
    "AantalInKuubskist": "Aantal_in_kuubskist",
    "AantalPerMeter": "Aantal_per_meter",
    "OmsCultivar": "Cultivar",
    "Plantgoed": "Plantgoed",
    "UitvalGeschat": "Uitval_geschat",
    "RooiDatum": "Rooidatum",
    "FustVolgnummer": "Fust_volgnummer",
    "FustVolgnummerPartij": "Fust_volgnummer_partij",
    "KgPerFust": "Kg_per_fust",
    "AantalFust": "Aantal_fust",
    "AantalPerFust": "Aantal_per_fust",
    "Partijnummer": "Partijnummer",
    "PartijVolgnummer": "Partij_volgnummer",
    "Fustnummer": "Fustnummer",
    "Inactief": "Inactief",
    "ReferentieMaat": "Referentie_maat",
    "UitMaat": "Uit_maat",
    "OmsPerceel": "Omschrijving_perceel",
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