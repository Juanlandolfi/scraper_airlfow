#from __future__ import annotations

import logging
from dotenv import load_dotenv
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import is_venv_installed

#LOADING ENVIROMENT VARIABLES
load_dotenv()


################################################

log = logging.getLogger(__name__)

if not is_venv_installed():
    log.warning("The task_with_virtualenv example DAG requires virtualenv, please install it.")
else:

    @dag(schedule='@weekly', 
         start_date=datetime(2024, 3, 1), 
         catchup=False, 
         description="Kilbel Scraper tasks",
         tags=["scrapper"])
    def scraper_kilbel_dag():
        """
        DAG for extracting and save in database
        """

        @task.virtualenv(
            use_dill=True,
            system_site_packages=False,
            requirements=[
                "python-dotenv==1.0.1",
                "asttokens==2.4.1",
                "beautifulsoup4==4.12.3",
                "certifi==2023.11.17",
                "charset-normalizer==3.3.2",
                "comm==0.2.1",
                "debugpy==1.8.0",
                "decorator==5.1.1",
                "exceptiongroup==1.2.0",
                "executing==2.0.1",
                "greenlet==3.0.3",
                "idna==3.6",
                "ipykernel==6.29.0",
                "ipython==8.20.0",
                "jedi==0.19.1",
                "jupyter_client==8.6.0",
                "jupyter_core==5.7.1",
                "matplotlib-inline==0.1.6",
                "mysql-connector-python==8.3.0",
                "nest-asyncio==1.6.0",
                "packaging==23.2",
                "parso==0.8.3",
                "pexpect==4.9.0",
                "platformdirs==4.1.0",
                "prompt-toolkit==3.0.43",
                "psutil==5.9.8",
                "ptyprocess==0.7.0",
                "pure-eval==0.2.2",
                "Pygments==2.17.2",
                "python-dateutil==2.8.2",
                "pyzmq==25.1.2",
                "requests==2.31.0",
                "six==1.16.0",
                "soupsieve==2.5",
                "SQLAlchemy==2.0.25",
                "stack-data==0.6.3",
                "tornado==6.4",
                "traitlets==5.14.1",
                "typing_extensions==4.9.0",
                "urllib3==2.1.0",
                "wcwidth==0.2.13"
            ],
        )
        # @task.external_python(task_id="scraper_venv", 
        #                       python='/opt/airflow/venvscraper/bin/python3',
        #                       expect_airflow=False)
        def run_scrapper():
            # Connect with Database
            import requests
            import re
            import json
            import os

            from bs4 import BeautifulSoup
            from sqlalchemy import create_engine, MetaData

            ## IMPORT SETTING
            SETTIGNS = json.load(open('settings.json'))
            # DB = SETTIGNS["DATABASE"]
            # USER_DB, PASSWORD_DB, HOST_DB, PORT_DB, DATABASE_NAME = DB.values()


            USER_DB=os.getenv("USER_DB")
            PASSWORD_DB=os.getenv("PASSWORD_DB")
            HOST_DB=os.getenv("HOST_DB")
            PORT_DB=os.getenv("PORT_DB")
            DATABASE_NAME=os.getenv("DATABASE_NAME")


            DATABASE_URL = f"mysql+mysqlconnector://{USER_DB}:{PASSWORD_DB}@{HOST_DB}:{PORT_DB}/{DATABASE_NAME}"

            # Get url
            BASEURL = SETTIGNS['BASEURL']
            PATH_COMPLEMENT = SETTIGNS['PATH_COMPLEMENT']

            # Get pages
            # print(DATABASE_URL)
            # print(BASEURL, PATH_COMPLEMENT)
            engine = create_engine(DATABASE_URL)
            # Get MetaData and Reflect Database structure
            metadata = MetaData()
            metadata.reflect(bind=engine)

            # Load table
            prices_table = metadata.tables['kilbel_prices']

            # Pages and Products

            response_kilbel = requests.get(BASEURL + PATH_COMPLEMENT.replace("%%PAGE_CURRENT%%", "1"), timeout=60*5)
            soup = BeautifulSoup(response_kilbel.text,  "html.parser")
            try:
                last_page = soup.find('span', {'class':'icono pag_ultima'}).parent.attrs['href'].split('/')[-2]
            except AttributeError:
                last_page = 1
            last_page = int(last_page) 
            PAGES = range(1,last_page+1)

            def get_page_and_products(BASEURL, PATH_COMPLEMENT, CURRENT_PAGE) -> list:
                PATH_COMPLEMENT = PATH_COMPLEMENT.replace("%%PAGE_CURRENT%%", f'{CURRENT_PAGE}')
                response_kilbel = requests.get(BASEURL + PATH_COMPLEMENT, timeout=60*5)
                # Parse with bs4
                soup = BeautifulSoup(response_kilbel.text,  "html.parser")

                # Select Products Wrapper
                product_wrapper = soup.find('div', {'class':'caja1 producto'})

                # Return list of products by ids
                return product_wrapper.find_all('div',{'id':re.compile("^prod_")})

            # Helper Function
            def clean_price(string_price: str) -> float:
                return float(string_price.replace('$', '').replace('.','').replace(',','.'))

            # Scrape and save products
            def scrape_and_save_products(products, engine):
                # Iterate over products
                for product in products:
                    # For each product we...
                    # 1 - Get ID_Product_store
                    product_id = product.attrs['id']

                    # 2 - Get Product Name
                    product_name = product.find('div', {'class': 'titulo02 aux1 titulo_puntos clearfix'}).attrs['title']

                    # 3 - Get current price
                    try: 
                        product_current_price = product.find('div', {'class': 'precio aux1'}).text
                    except AttributeError:
                        product_current_price = product.find('span', {'class': 'precio aux1'}).text

                    # 4 - Get regular price
                    try:
                        product_regular_price = product.find('div', {'class': 'precio anterior codigo'}).text
                    except AttributeError:
                        product_regular_price = product_current_price
                    # 5 - Clean prices
                    product_current_price = clean_price(product_current_price)
                    product_regular_price = clean_price(product_regular_price)

                    # 6 - Create Insert Query
                    insert_query = prices_table.insert().values(
                                            store_name=BASEURL, 
                                            store_product_code=product_id,
                                            product_description=product_name,
                                            product_price_regular=product_regular_price,
                                            product_price_ofert=product_current_price
                                            )
                    # 7 - Commit to database
                    with engine.connect() as connection:
                        connection.execute(insert_query)
                        connection.commit()

            ### Run
            ### Iterates over pages, scrapes and saves data
            for page_number in PAGES:
                products = get_page_and_products(BASEURL,PATH_COMPLEMENT, page_number)
                scrape_and_save_products(products, engine)

                
        run_scrapper()


       
    tutorial_dag = scraper_kilbel_dag()