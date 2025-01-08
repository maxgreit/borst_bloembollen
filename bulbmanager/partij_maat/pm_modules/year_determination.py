from datetime import datetime

def recent_years():

    huidig_jaar = datetime.now().year
    vorig_jaar = huidig_jaar - 1
    teeltjaren = [vorig_jaar, huidig_jaar]
    
    return teeltjaren

def past_years():

    twee_jaar_terug = datetime.now().year - 2
    eerste_jaar = 2013
    teeltjaren = range(twee_jaar_terug, eerste_jaar, -1)
    
    return teeltjaren