import logging
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs

logging.basicConfig(level=logging.INFO)
logging.info("Loading input data...")

input_data = pd.read_csv("./data/input.csv", header=None)
output_data = []

logging.info("Input data loaded, data fetching has started...")

for index, row in input_data.iterrows():
    try:
        url = row[0]
        vehicle_id = url.strip("https://www.cars.com/vehicledetail/")
        logging.info(f"Fetching vehicle {vehicle_id}...")
    
        response = requests.get(url)
    
        if response.status_code != 200:
            logging.error(f"Could not fetch details for {vehicle_id}, moving onto the next vehicle...")
            continue
    
        logging.info("Vehicle retrieved, parsing...")
    
        soup = bs(response.text, "html.parser")
        basics_section = soup.find("section", {"class": "basics-section"})
        description_list = basics_section.find('dl')
    
        keys = [value.get_text().strip().lower() for value in description_list.find_all('dt')]
        values = [value.get_text().strip() for value in description_list.find_all('dd')]
        description_map = dict(zip(keys, values))
    
        row_data = {
            "id": vehicle_id,
            "VIN": description_map.get("vin"),
            "Stock Number": description_map.get("stock #"),
            "Color": description_map.get("exterior color"),
            "Mileage": "".join(re.findall(r'\d+', description_map.get("mileage"))),
            "Year": "".join(re.findall(r'\d+', soup.find("h1", {"class": "listing-title"}).get_text())),
            "Listing Name": soup.find("h1", {"class": "listing-title"}).get_text(),
            "Price": "".join(re.findall(r'\d+', soup.find("span", {"class": "primary-price"}).get_text())),
            "URL": url,
        }
        
        logging.info("Parsing successful!")
    
        output_data.append(row_data)
        
    except Exception as e:
        logging.error(f"Could not fetch details for {vehicle_id}, moving onto the next vehicle...")
        logging.error(str(e))
        continue

logging.info("Outputting results...")
pd.DataFrame(output_data).to_csv("./data/output.csv", encoding="utf-8", index=False)
logging.info("Results written successfully!")
