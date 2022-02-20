
import requests
import logging
import os
import pandas as pd
from util_libs.config import ConfigFunctions 
from datetime import datetime
from dotenv import load_dotenv
from os import getenv

load_dotenv("/home/ubuntu/.env")

# Declaration of variables
data_source = 'anp_fuel_sales'
bucket = getenv("AWS_BUCKET")
today = datetime.now().strftime("%Y/%m/%d")
bucket_name=f'raw/{data_source}/{today}/'

logger = logging.getLogger('root')
formatt='%(asctime)s - %(name)s - %(levelname)s - %(message)s'

logging.basicConfig(level=logging.INFO, 
                    format=formatt,
                    datefmt='%Y-%m-%d %H:%M:%S')

logger.info(f'Getting file')
url='https://github.com/raizen-analytics/data-engineering-test/raw/master/assets/vendas-combustiveis-m3.xls'
file_origin = url.split('/')[-1]

r = requests.get(url)

# Download the file and call the conversion function
def get_file_source():
    open(url.split('/')[-1], 'wb').write(r.content)
    ConfigFunctions.convert_file(file_origin, bucket_name, logger=logger)
    os.remove(file_origin) 

# Call the file download function
get_file_source()

# Calls the parquet conversion function, passing as a parameter the sheet number that contains the desired cache of the pivot table.
ConfigFunctions.convert_parquet('sales_of_oil_derivative_fuels.parquet', 1, file_origin, bucket, bucket_name, logger)
ConfigFunctions.convert_parquet('sales_of_diesel.parquet', 2, file_origin, bucket, bucket_name, logger)

