#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE IF NOT EXISTS mock_data (
        id INT,
        customer_first_name TEXT,
        customer_last_name TEXT,
        customer_age INT,
        customer_email TEXT,
        customer_country TEXT,
        customer_postal_code TEXT,
        customer_pet_type TEXT,
        customer_pet_name TEXT,
        customer_pet_breed TEXT,
        seller_first_name TEXT,
        seller_last_name TEXT,
        seller_email TEXT,
        seller_country TEXT,
        seller_postal_code TEXT,
        product_name TEXT,
        product_category TEXT,
        product_price DOUBLE PRECISION,
        product_quantity INT,
        sale_date TEXT,
        sale_customer_id INT,
        sale_seller_id INT,
        sale_product_id INT,
        sale_quantity INT,
        sale_total_price DOUBLE PRECISION,
        store_name TEXT,
        store_location TEXT,
        store_city TEXT,
        store_state TEXT,
        store_country TEXT,
        store_phone TEXT,
        store_email TEXT,
        pet_category TEXT,
        product_weight TEXT,
        product_color TEXT,
        product_size TEXT,
        product_brand TEXT,
        product_material TEXT,
        product_description TEXT,
        product_rating TEXT,
        product_reviews TEXT,
        product_release_date TEXT,
        product_expiry_date TEXT,
        supplier_name TEXT,
        supplier_contact TEXT,
        supplier_email TEXT,
        supplier_phone TEXT,
        supplier_address TEXT,
        supplier_city TEXT,
        supplier_country TEXT
    );
EOSQL

for f in /data/mock_data_*.csv; do
    echo "--- Loading $f ---"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -c "COPY mock_data FROM '$f' DELIMITER ',' CSV HEADER;"
done