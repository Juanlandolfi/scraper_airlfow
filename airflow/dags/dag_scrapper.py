#from __future__ import annotations

import logging
from dotenv import load_dotenv
from datetime import datetime
from airflow.decorators import dag, task

#LOADING ENVIROMENT VARIABLES
load_dotenv()


################################################

log = logging.getLogger(__name__)

@dag(schedule='@weekly', 
    start_date=datetime(2024, 3, 4), 
    catchup=False, 
    description="Kilbel Scraper tasks",
    tags=["scrapper"])
def scraper_kilbel_dag():
    """
    DAG for extracting and save in database
    """
    @task
    def get_products_html():    
        # Imports
        import requests
        import re
        from bs4 import BeautifulSoup
        from airflow.models import Variable
        # Get url
        BASEURL = Variable.get('BASE_URL_KILBEL')
        PATH_COMPLEMENT = Variable.get('COMPLEMENT_URL_KILBEL')

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
        
        # get list of products wrappers html of all pages
        products = [get_page_and_products(BASEURL,PATH_COMPLEMENT,curr_page) for curr_page in PAGES]
        # Flatten list to get one product per position
        products_flat = [product for wrapper in products for product in wrapper] 
        
        # Helper Function
        def clean_price(string_price: str) -> float:
            return float(string_price.replace('$', '').replace('.','').replace(',','.'))
        final_list = []
        for product in products_flat:
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
                final_list.append(
                        dict(
                            store_name=BASEURL,
                            store_product_code= product_id,
                            product_description=product_name,
                            product_price_regular=product_regular_price,
                            product_price_ofert=product_current_price
                        )
                    )
        return final_list
    
    @task
    def save_products_to_database(data) -> None:
        """Save products to database"""
        import os
        from sqlalchemy import create_engine, MetaData
        from sqlalchemy.orm import Session

        USER_DB=os.getenv("USER_DB")
        PASSWORD_DB=os.getenv("PASSWORD_DB")
        HOST_DB=os.getenv("HOST_DB")
        PORT_DB=os.getenv("PORT_DB")
        DATABASE_NAME=os.getenv("DATABASE_NAME")


        DATABASE_URL = f"mysql+mysqlconnector://{USER_DB}:{PASSWORD_DB}@{HOST_DB}:{PORT_DB}/{DATABASE_NAME}"

        # Get pages
        engine = create_engine(DATABASE_URL)
        # Get MetaData and Reflect Database structure
        metadata = MetaData()
        metadata.reflect(bind=engine)
        # Load table
        prices_table = metadata.tables['kilbel_prices']
        
        with Session(engine) as session:
            session.execute(prices_table.insert(), data)
            session.commit()


    # run_scrapper()
    save_products_to_database(get_products_html())



screaper_dag = scraper_kilbel_dag()
