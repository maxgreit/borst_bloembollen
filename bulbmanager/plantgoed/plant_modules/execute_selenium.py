from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementNotInteractableException
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from plant_modules.config import determine_script_id
from plant_modules.env_tool import env_check
from plant_modules.log import setup_logging
import logging
import time
import os

def wait_and_click(driver, xpath, element):
    # Knop klikken
    try:
        WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, xpath))
            )
        logging.info(f"{element} zichtbaar.")
        time.sleep(2)
        button = driver.find_element(By.XPATH, xpath)
        button.click()
        logging.info(f"{element} geklikt.")
    except Exception as e:
        logging.error(f"{str(e)}")
            
def wait_and_hover(driver, xpath, element_name):
    # Hover boven het element
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        logging.info(f"{element_name} zichtbaar.")
        element = driver.find_element(By.XPATH, xpath)
        actions = ActionChains(driver)
        actions.move_to_element(element).perform()
        logging.info(f"Gehoverd boven {element_name}.")
        time.sleep(2)
    except Exception as e:
        logging.error(f"Fout bij hoveren: {str(e)}")

def download_plantgoed_file(url, bm_username, bm_password, base_dir, jaar):
    
    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")  # Disables GPU hardware acceleration (optioneel)
    chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--no-sandbox")  # Zorgt ervoor dat je script kan draaien in een beveiligde omgeving (optioneel)

    # Instellen van downloadopties
    download_dir = os.path.join(base_dir, "bulbmanager/plantgoed/bestand")
    
    # Instellen van downloadopties
    prefs = {
        "download.default_directory": download_dir,  # Pas dit pad aan naar de gewenste map
        "download.prompt_for_download": False,  # Geen download prompt
        "download.directory_upgrade": True,  # Upgrade de directory indien nodig
        "safebrowsing.enabled": True  # Zorg ervoor dat Safe Browsing ingeschakeld is
    }
    chrome_options.add_experimental_option("prefs", prefs)

    # WebDriver configureren met deze opties
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.set_window_size(1920, 1080)  # Pas aan naar de gewenste grootte

    try:
        # Navigeer naar URL
        try:
            driver.get(url)
            logging.info(f"URL {url} geopend.")
        except Exception as e:
            logging.error(f"Fout bij het openen van de URL: {str(e)}")

        # Pagina laten laden
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//input[@name='Email']")))
            logging.info("Pagina geladen, email veld gevonden.")
        except Exception as e:\
            logging.error(f"Fout bij het wachten tot de pagina is geladen: {str(e)}")
            
        # Inloggegevens invullen
        try:
            username = driver.find_element(By.XPATH, "//input[@name='Email']")
            password = driver.find_element(By.XPATH, "//input[@name='password']")
            username.send_keys(bm_username)
            password.send_keys(bm_password)
            logging.info("Inloggegevens ingevuld.")
        except Exception as e:
            logging.error(f"Fout bij het invullen van inloggegevens: {str(e)}")
        
        # Inloggen
        wait_and_click(driver, "//button[span[text()='Login']]", "Inloggen")

        # Menu openen
        wait_and_click(driver, '//*[@id="app"]/div[1]/div/div[1]/div/header/div/button[1]/span/i', "Menu")
        
        # Overzichten openen
        wait_and_click(driver, '//*[@id="app"]/div[2]/div/div[1]/div/nav[1]/div[1]/div/div[11]/div/div[2]/div', "Overzichten")

        # Track & Trace openen
        wait_and_click(driver, '//*[@id="app"]/div[2]/div/div[1]/div/nav[1]/div[1]/div/div[11]/div[2]/div[1]/div/div[2]/div', "Overzichten")
        
        # Plantgoed openen
        wait_and_click(driver, '//*[@id="app"]/div[2]/div/div[1]/div/nav[1]/div[1]/div/div[11]/div[2]/div[1]/div[2]/a[3]/div', "Plantgoed")
        
        # Gewassen scherm sluiten
        time.sleep(2)
        button = driver.find_elements(By.XPATH, '//*[@id="app"]/div[4]/div/div/div[3]/button/span')
        if button:
            wait_and_click(driver, '//*[@id="app"]/div[4]/div/div/div[3]/button/span', "Gewassen scherm")
        
        # Alle gewassen lijst openen
        wait_and_click(driver, '//*[@id="app"]/div[1]/div/div[1]/div/header/div/button[4]', "Gewassen lijst")

        # Tulipa gewas kiezen
        wait_and_click(driver, "//div[@class='v-list-item__title' and text()='Tulipa']", "Tulipa gewas")
        
        '''# Filter reset hover
        wait_and_hover(driver, '//*[@id="app"]/div[1]/div/div[1]/main/div/div/div[1]/div[2]/button[5]', "Filter reset")
        
        # Alles selecteren
        wait_and_click(driver, "//div[@class='v-list-item__title' and text()='Alles']/parent::div", "Alles selecteren")'''
        
        # Plantgoed deselecteren
        time.sleep(2)
        button = driver.find_elements(By.XPATH, '//*[@id="app"]/div[1]/div/div[1]/main/div/div/div[2]/div[12]/div/div/div/div[1]/div[1]/div/button')
        if button:
            wait_and_click(driver, '//*[@id="app"]/div[1]/div/div[1]/main/div/div/div[2]/div[12]/div/div/div/div[1]/div[1]/div/button', "Plantgoed deselectie")
        else:
            logging.info("Plantgoed al gedeselecteerd.")
        
        # Status deselecteren
        time.sleep(2)
        button = driver.find_elements(By.XPATH, '//*[@id="app"]/div[1]/div/div[1]/main/div/div/div[2]/div[13]/div/div/div/div[1]/div[1]/div/button')
        if button:
            wait_and_click(driver, '//*[@id="app"]/div[1]/div/div[1]/main/div/div/div[2]/div[13]/div/div/div/div[1]/div[1]/div/button', "Status deselectie")
        else:
            logging.info("Status al gedeselecteerd.")
        
        # Geplant dropdown
        wait_and_click(driver, '//*[@id="app"]/div[1]/div/div[1]/main/div/div/div[2]/div[16]/div/div/div/div[1]/div[2]/div', "Geplant dropdown")
        
        # Geplant Ja
        wait_and_click(driver, "//div[@class='v-list-item__title' and text()='Ja']", "Geplant Ja")
        
        # Wachten
        time.sleep(10)
        
        # Jaren dropwdown
        wait_and_click(driver, '//*[@id="app"]/div[1]/div/div[1]/div/header/div/button[6]', "Jaren dropdown")
    
        # Jaar selecteren
        wait_and_click(driver, f'//div[@class="v-list-item__title" and text()="{jaar}"]', f"Jaar {jaar} selecteren")
  
        # Wachten
        time.sleep(5)
        
        # Excel bestand downloaden
        wait_and_click(driver, '//*[@id="app"]/div[1]/div/div[1]/main/div/div/div[1]/div[2]/button[3]/span', "Excel knop")

        # Wachten op het downloaden van het bestand
        default_filename = "plantgoedoverzicht.xlsx" 
        new_filename = f"Plantgoed_{jaar}.xlsx"
        timeout = 30
        start_time = time.time()

        # Controle van download
        while not os.path.exists(os.path.join(download_dir, default_filename)):
            if time.time() - start_time > timeout:
                logging.error("Geen bestand gevonden, downloaden duurde mogelijk te lang.")
                break
            time.sleep(1)
        
        # Hernoemen van bestand
        if os.path.exists(os.path.join(download_dir, default_filename)):
            new_file_path = os.path.join(download_dir, new_filename)
            os.rename(
            os.path.join(download_dir, default_filename),
            new_file_path
            )
            logging.info("Bestand succesvol gedownload en hernoemd!")

    finally:
        # Browser sluiten
        try:
            driver.quit()
            logging.info("Browser gesloten.")
        except Exception as e:
            logging.error(f"Fout bij het sluiten van de browser: {str(e)}")
        
            
if __name__ == "__main__":
    # Lokaal of productieomgeving bepaling
    env_check()

    # Script configuratie
    klant = "Borst"
    script = "Plantgoed | Recent"
    bron = 'Bulbmanager'
    start_time = time.time()

    # Omgevingsvariabelen
    server = os.getenv('SERVER')
    database = os.getenv('DATABASE')
    username = os.getenv('GEBRUIKERSNAAM')
    password = os.getenv('PASSWORD')
    driver = '{ODBC Driver 18 for SQL Server}'
    greit_connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=no;TrustServerCertificate=no;Connection Timeout=30;'

    # Script ID bepalen
    script_id = determine_script_id(greit_connection_string, klant, bron, script)
    
    setup_logging(greit_connection_string, klant, bron, script, script_id, log_file='app.log', log_level=logging.INFO)
    download_plantgoed_file()