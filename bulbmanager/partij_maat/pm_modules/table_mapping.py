import logging

fust_details = {
    "OmsPartij": "Partij",
    "OmsPartijKort": "Partij_afkorting",
    "PartijAanduiding": "Partij_aanduiding",
    "Tjr": "Teeltjaar",
    "AantalInKuubskist": "Aantal_in_kuubskist",
    "AantalPerMeter": "Aantal_per_meter",
    "OmsCultivar": "Cultivar",
    "UitvalGeschat": "Uitval_geschat",
    "RooiDatum": "Rooi_datum",
    "RooiDatumTijdstip": "Rooi_datumtijd",
    "OntvangstDatum": "Ontvangst_datum",
    "OntvangstDatumTijdstip": "Ontvangst_datumtijd",
    "PelDatumGepland": "Pel_datum_gepland",
    "PelDatumWerkelijk": "Pel_datum_werkelijk",
    "PelDatumTijdstip": "Pel_datumtijd",
    "SpoeldatumWerkelijk": "Spoel_datum_werkelijk",
    "DatumGezeefd": "Gezeefd_datum",
    "DatumUitgezocht": "Uitgezocht_datum",
    "SpoelDatumTijdstip": "Spoel_datumtijd",
    "DroogTijdstipStart": "Droog_datumtijd_start",
    "DroogTijdstipEind": "Droog_datumtijd_eind",
    "DatumAfgehandeld": "Afgehandeld_datum",
    "DatumTijdstipAfgehandeld": "Afgehandeld_datumtijd",
    "StoomTijdstipStart": "Stoom_datumtijd_start",
    "StoomTijdstipEind": "Stoom_datumtijd_eind",
    "FustVolgnummer": "Fust_volgnummer",
    "FustVolgnummerPartij": "Fust_volgnummer_partij",
    "KgPerFust": "KG_per_fust",
    "AantalFust": "Aantal_fust",
    "AantalPerFust": "Aantal_per_fust",
    "SpoelTijdstip": "Spoel_tijd",
    "Timestamp": "Datumtijd",
    "Minus7": "Minus_7",
    "PartijVolgnummer": "Partij_volgnummer",
    "ReferentieMaat": "Referentie_maat",
    "UitMaat": "Uit_maat",
    "OmsPerceel": "Perceel",
    "GewasGroep": "Gewas_groep",
    "AanmaakLocatie": "Aanmaak_locatie",
    "TotaalBollen": "Totaal_bollen",
    "RestantBollen": "Restant_bollen"
}

partij_maat_details = {
    "OmsCultivar": "Cultivar",
    "Afk": "Afkorting",
    "KwekersRechthouder": "Kwekers_rechthouder",
    "KostenRRPerJaar": "Kosten_RR_per_jaar",
    "VolgordeBroeien": "Volgorde_broeien",
    "PreparatieTussenTemp": "Preparatie_tussen_temp",
    "PreparatieKoudeWeken": "Preparatie_koude_weken",
    "KasDagen": "Kas_dagen",
    "BesteBroeiPeriode": "Beste_broei_periode",
    "HuidKwaliteit": "Huid_kwaliteit",
    "ZuurGevoeligheid": "Zuur_gevoeligheid",
    "KernRotGevoeligheid": "Kern_rot_gevoeligheid",
    "PloidyNiveau": "Ploidy_niveau",
    "AanbodKg": "Aanbod_KG",
    "PrijsPerKg": "Prijs_per_KG",
    "StartDatumWBSO": "Start_datum_WBSO",
    "EindDatumWBSO": "Eind_datum_WBSO",
    "StartOogstjaarWBSO": "Start_oogstjaar_WBSO",
    "EindOogstjaarWBSO": "Eind_oogstjaar_WBSO",
    "SelectieType": "Selectie_type",
    "JaarSelectie": "Jaar_selectie",
    "BloeiPeriode": "Bloei_periode",
    "PootLengte": "Poot_lengte",
    "RooiPeriode": "Rooi_periode",
    "IdealeKoudeBehoefte": "Ideale_koud_behoefte",
    "BewaarTemperatuurShow": "Bewaar_temperatuur_show",
    "PlantMethode": "Plant_methode",
    "UitgelichtBloemen": "Uitgelicht_bloemen",
    "UitgelichtBollen": "Uitgelicht_bollen",
    "BloembollenAanbod": "Bloembollen_aanbod",
    "PlantgoedAanbod": "Plantgoed_aanbod",
    "ActiefPlantgoed": "Actief_plantgoed",
    "KleurID": "Kleur_ID",
    "OmsOrigineel": "Origineel",
    "LijnKleurPolygoon": "Lijn_kleur_polygoon",
    "OmsCultivarVorig1": "Vorige_cultivar",
    "JaarEindeSoort": "Jaar_einde_soort",
    "PlantTimeout": "Plant_timeout",
    "VerkoopOpKleur": "Verkoop_op_kleur",
    "CultivarAfk": "Cultivar_afkorting",
    "DeadlinePellen": "Deadline_pellen",
    "Tjr": "Teeltjaar",
    "TotaalRRPlantgoed": "Totaal_RR_plantgoed",
    "TotaalRRLeverbaar": "Totaal_RR_leverbaar",
    "ReferentieMaat": "Referentie_maat",
    "CultivarMaatTotaalBollenVerkocht": "Cultivar_maat_totaal_bollen_verkocht"
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

def apply_mapping(df, tabelnaam):
    # Kolom mapping
    column_mapping = {
        'Partij_maat': partij_maat_details,
        'Fust': fust_details
    }

    # Tabel mapping
    for mapping_table, mapping in column_mapping.items():
        if tabelnaam == mapping_table:

            # Transformeer de kolommen
            try:
                transformed_df = transform_columns(df, mapping)
                logging.info(f"Kolommen getransformeerd")
                
                return transformed_df
            except Exception as e:
                logging.error(f"Kolommen transformeren mislukt: {e}")

            
            