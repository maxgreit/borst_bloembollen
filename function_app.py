import sys
import os
import logging
import azure.functions as func
import pyodbc
import time
from datetime import datetime, timedelta

# Configureer logging
logging.basicConfig(level=logging.INFO)

# Verkrijg het absolute pad naar de root van je project
project_root = os.path.dirname(os.path.abspath(__file__))

# Voeg absolute paden toe aan sys.path voor dagelijkse, maandelijkse en werknemers scripts
sys.path.append(os.path.join(project_root, 'bulbmanager'))
from bulbmanager.bm_main import main as bm_main
from bulbmanager.bm_modules.year_determination import recent_years, past_years

sys.path.append(os.path.join(project_root, 'dyflexis', 'registered_hours'))
from dyflexis.registered_hours.hour_main import main as hour_main
from dyflexis.registered_hours.hour_modules.year_determination import this_year, last_year

sys.path.append(os.path.join(project_root, 'dyflexis', 'employees'))
from dyflexis.employees.empl_main import main as employees_main

# Azure Function App object
app = func.FunctionApp()

@app.function_name(name="BmDagelijks")
@app.schedule(schedule="0 0 4 * * *", arg_name="BmDagelijkseTimer", run_on_startup=False, use_monitor=True)
def bm_dagelijks(BmDagelijkseTimer: func.TimerRequest) -> None:
    teeltjaren = recent_years()
    run_script(bm_main, "Bulbmanager | Dagelijks", teeltjaren)

@app.function_name(name="BmWekelijks")
@app.schedule(schedule="0 10 4 1 * *", arg_name="BmWekelijkseTimer", run_on_startup=False, use_monitor=True)
def bm_wekelijks(BmWekelijkseTimer: func.TimerRequest) -> None:
    teeltjaren = past_years()
    run_script(bm_main, "Bulbmanager | Wekelijks", teeltjaren)

@app.function_name(name="UrenDagelijks")
@app.schedule(schedule="0 20 4 * * *", arg_name="UrenDagelijkseTimer", run_on_startup=False, use_monitor=True)
def uren_dagelijks(UrenDagelijkseTimer: func.TimerRequest) -> None:
    datum_range = this_year()
    run_script(hour_main, "Dyflexis | Geregistreerde Uren | Dagelijks", datum_range)

@app.function_name(name="UrenMaandelijks")
@app.schedule(schedule="0 30 4 1 * *", arg_name="UrenMaandelijksTimer", run_on_startup=False, use_monitor=True)
def uren_maandelijks(UrenMaandelijksTimer: func.TimerRequest) -> None:
    datum_range = last_year()
    run_script(hour_main, "Productiviteits Dashboard Uren | Maandelijks", datum_range)

@app.function_name(name="WerknemersWekelijks")
@app.schedule(schedule="0 40 4 * * 1", arg_name="WerknemersWekelijkseTimer", run_on_startup=False, use_monitor=True)
def werknemers_wekelijks(WerknemersWekelijkseTimer: func.TimerRequest) -> None:
    run_script(employees_main, "Productiviteits Dashboard Werknemers")

def run_script(script_main_function, script_type, script_input=None):
    try:
        logging.info(f"Start {script_type} script")
        
        # Begin tijdstip van het script
        start_time = time.time()
        
        # Log het begin van de hoofdfunctie
        logging.info(f"{script_type} | Uitvoeren van de main-functie")
        script_main_function(script_input)

        # Log het succesvol afronden van de main-functie
        logging.info(f"{script_type} | Main-functie succesvol afgerond")

        # Eind tijdstip van het script
        end_time = time.time()
        duration = timedelta(seconds=(end_time - start_time))
        
        # Log de totale looptijd van het script
        logging.info(f"{script_type} script is succesvol uitgevoerd in {str(duration)}")
        
    except Exception as e:
        # Log specifieke fouten en het type script waar de fout optrad
        logging.error(f"FOUTMELDING | {script_type} script is jammer genoeg mislukt: {e}")
        logging.info(f"{script_type} | Script geëindigd met fouten")
    finally:
        # Dit deel wordt altijd uitgevoerd, ook als er een fout is
        logging.info(f"Einde van {script_type} script")