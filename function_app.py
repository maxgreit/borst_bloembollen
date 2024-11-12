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
sys.path.append(os.path.join(project_root, 'cultivar_dashboard', 'cultivar_dashboard_dagelijks'))
from cultivar_dashboard.cultivar_dashboard_dagelijks.cultivar_dashboard_dagelijks import main as daily_main

sys.path.append(os.path.join(project_root, 'cultivar_dashboard', 'cultivar_dashboard_maandelijks'))
from cultivar_dashboard.cultivar_dashboard_maandelijks.cultivar_dashboard_maandelijks import main as monthly_main

sys.path.append(os.path.join(project_root, 'productiviteits_dashboard', 'productiviteits_dashboard_geregistreerde_uren', 'productiviteits_dashboard_geregistreerde_uren_dagelijks'))
from productiviteits_dashboard.productiviteits_dashboard_geregistreerde_uren.productiviteits_dashboard_geregistreerde_uren_dagelijks.productiviteits_dashboard_geregistreerde_uren_dagelijks import main as productivity_daily_main

sys.path.append(os.path.join(project_root, 'productiviteits_dashboard', 'productiviteits_dashboard_geregistreerde_uren', 'productiviteits_dashboard_geregistreerde_uren_maandelijks'))
from productiviteits_dashboard.productiviteits_dashboard_geregistreerde_uren.productiviteits_dashboard_geregistreerde_uren_maandelijks.productiviteits_dashboard_geregistreerde_uren_maandelijks import main as productivity_monthly_main

sys.path.append(os.path.join(project_root, 'productiviteits_dashboard', 'productiviteits_dashboard_werknemers'))
from productiviteits_dashboard.productiviteits_dashboard_werknemers.productiviteits_dashboard_werknemers import main as productivity_weekly_main

# Azure Function App object
app = func.FunctionApp()

# Dagelijkse run (elke dag om 4 uur 's nachts)
@app.function_name(name="CultivarDashboardDagelijks")
@app.schedule(schedule="0 0 4 * * *", arg_name="CultivarDashboardDagelijksTimer", run_on_startup=False, use_monitor=True)
def Cultivar_dagelijks(CultivarDashboardDagelijksTimer: func.TimerRequest) -> None:
    
    run_script(daily_main, "Cultivar Dashboard | Dagelijks")

# Maandelijkse run (elke eerste dag van de maand om 4 uur 's nachts)
@app.function_name(name="CultivarDashboardMaandelijks")
@app.schedule(schedule="0 10 4 1 * *", arg_name="CultivarDashboardMaandelijksTimer", run_on_startup=False, use_monitor=True)
def Cultivar_maandelijkse(CultivarDashboardMaandelijksTimer: func.TimerRequest) -> None:
    run_script(monthly_main, "Cultivar Dashboard | Maandelijks")

# Productiviteitsmeting - dagelijks (elke dag om 4 uur 's nachts)
@app.function_name(name="ProductiviteitsDashboardUrenDagelijks")
@app.schedule(schedule="0 20 4 * * *", arg_name="ProductiviteitsDashboardUrenDagelijksTimer", run_on_startup=False, use_monitor=True)
def Productiviteit_urenregistratie_dagelijks(ProductiviteitsDashboardUrenDagelijksTimer: func.TimerRequest) -> None:
    run_script(productivity_daily_main, "Productiviteits Dashboard Uren | Dagelijks")

# Productiviteitsmeting - maandelijks (elke eerste dag van de maand om 4 uur 's nachts)
@app.function_name(name="ProductiviteitsDashboardUrenMaandelijks")
@app.schedule(schedule="0 30 4 1 * *", arg_name="ProductiviteitsDashboardUrenMaandelijksTimer", run_on_startup=False, use_monitor=True)
def Productiviteit_urenregistratie_maandelijks(ProductiviteitsDashboardUrenMaandelijksTimer: func.TimerRequest) -> None:
    run_script(productivity_monthly_main, "Productiviteits Dashboard Uren | Maandelijks")

# Productiviteitsmeting - werknemers (elke maandag om 4 uur 's nachts)
@app.function_name(name="ProductiviteitsDashboardWerknemers")
@app.schedule(schedule="0 40 4 * * 1", arg_name="ProductiviteitsDashboardWerknemersTimer", run_on_startup=False, use_monitor=True)
def Productiviteit_werknemers_wekelijks(ProductiviteitsDashboardWerknemersTimer: func.TimerRequest) -> None:
    run_script(productivity_weekly_main, "Productiviteits Dashboard Werknemers")

def run_script(script_main_function, script_type):
    try:
        logging.info(f"Start {script_type} script")
        
        # Begin tijdstip van het script
        start_time = time.time()
        
        # Log het begin van de hoofdfunctie
        logging.info(f"{script_type} | Uitvoeren van de main-functie")
        script_main_function()

        # Log het succesvol afronden van de main-functie
        logging.info(f"{script_type} | Main-functie succesvol afgerond")

        # Eind tijdstip van het script
        end_time = time.time()
        duration = timedelta(seconds=(end_time - start_time))
        
        # Log de totale looptijd van het script
        logging.info(f"{script_type} script is succesvol uitgevoerd in {str(duration)}")
        
    except Exception as e:
        # Log specifieke fouten en het type script waar de fout optrad
        logging.error(f"FOUTMELDING | {script_type} script is mislukt: {e}")
        logging.info(f"{script_type} | Script geëindigd met fouten")
    finally:
        # Dit deel wordt altijd uitgevoerd, ook als er een fout is
        logging.info(f"Einde van {script_type} script")