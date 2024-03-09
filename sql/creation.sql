CREATE SCHEMA IF NOT EXISTS scrapper;

USE scrapper;

CREATE TABLE IF NOT EXISTS scrapper.Prices (
	row_id INT NOT NULL AUTO_INCREMENT,
    store_name VARCHAR(150) NOT NULL,
    store_product_code VARCHAR(50), 
    product_description VARCHAR(250) NOT NULL,
	product_price_regular FLOAT NOT NULL,
    product_price_ofert FLOAT NOT NULL,
    date_scrapped TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (row_id)
);