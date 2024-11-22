from datetime import datetime

def this_year():
    # Huidig jaar
    huidig_jaar = datetime.now().year
    
    # Datum range voor dit jaar
    datum_range = {
        huidig_jaar: [f"{huidig_jaar}-01-01", f"{huidig_jaar}-12-31"]
    }

    return datum_range

def last_year():
    # Vorig jaar
    huidig_jaar = datetime.now().year
    vorig_jaar = huidig_jaar - 1

    # Datum range voor vorig jaar
    datum_range = {
        vorig_jaar: [f"{vorig_jaar}-01-01", f"{vorig_jaar}-12-31"]
    }

    return datum_range
