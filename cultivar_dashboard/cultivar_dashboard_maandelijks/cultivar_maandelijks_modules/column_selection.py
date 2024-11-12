partij_maat_columns = [
    "Cultivar", "Kleur", "Beschrijving", "Beschikbaarheid", 
    "Zuur_gevoeligheid", "Kern_rot_gevoeligheid", "Aanbod_kg", "Prijs_per_kg", 
    "Actief_plantgoed", "Inactief", "Teeltjaar", "Partij", 
    "Totaal_RR_plantgoed", "Totaal_RR_leverbaar", "Maat", "Referentie_maat", 
    "Cultivar_maat_totaal_bollen_verkocht"
]

fust_columns = [
    "Omschrijving_partij", "Teeltjaar", "Gesorteerd", 
    "Geteld", "Maat", "Aantal_in_kuubskist", "Aantal_per_meter", "Cultivar", 
    "Plantgoed", "Uitval_geschat", "Rooidatum", 
    "Fust_volgnummer", "Fust_volgnummer_partij", "Kg_per_fust", "Aantal_fust", 
    "Aantal_per_fust", "Partijnummer", "Partij_volgnummer", "Fustnummer", 
    "Inactief", "Referentie_maat", "Uit_maat", "Omschrijving_perceel"
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