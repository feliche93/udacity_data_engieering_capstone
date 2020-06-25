import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
fact_scraped_books_table_drop = "DROP TABLE IF EXISTS fact_scraped_books;"
fact_rebuy_sales_table_drop = "DROP TABLE IF EXISTS fact_rebuy_sales;"
dim_book_table_drop = "DROP TABLE IF EXISTS dim_book;"
dim_order_table_drop = "DROP TABLE IF EXISTS dim_order;"
dim_time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES
fact_scraped_books_table_create = ("""
    CREATE TABLE IF NOT EXISTS fact_scraped_books (
        scraped_timestamp TIMESTAMP NOT NULL,
        book_ean VARCHAR NOT NULL,
        rebuy_price DECIMAL,
        momox_price DECIMAL,
        rebuy_publication_year INTEGER,
        momox_publication_year INTEGER,
        spread DECIMAL,
        is_arbitrage INTEGER NOT NULL,
        momox_status VARCHAR,
        momox_referece_price DECIMAL,
        momox_warehouse_status DECIMAL,
        momox_demand_rating DECIMAL,
        rebuy_condition VARCHAR,
        rebuy_availability VARCHAR,
        PRIMARY KEY(scraped_timestamp)
    );
    """)

fact_rebuy_sales_table_create = ("""
    CREATE TABLE IF NOT EXISTS fact_rebuy_sales (
        sales_id SERIAL,
        order_id VARCHAR NOT NULL,
        order_date TIMESTAMP,
        book_ean VARCHAR,
        sales_price DECIMAL,
        shipping_cost DECIMAL,
        voucher_discount DECIMAL,
        PRIMARY KEY(sales_id)
    )
""")

dim_book_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_book (
        book_ean VARCHAR NOT NULL,
        title VARCHAR NOT NULL,
        categories VARCHAR,
        language VARCHAR,
        link VARCHAR,
        rating NUMERIC,
        number_of_ratings INT,
        author VARCHAR,
        product_form VARCHAR,
        publisher VARCHAR,
        decription TEXT,
        original_title VARCHAR,
        width DECIMAL,
        height DECIMAL,
        PRIMARY KEY(book_ean)
    );
""")

dim_order_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_order (
        order_id VARCHAR,
        status VARCHAR,
        number_of_packages INT,
        voucher_code VARCHAR,
        email VARCHAR,
        PRIMARY KEY(order_id)
    )
""")

dim_time_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_time (
        timestamp TIMESTAMP NOT NULL,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT
    );
""")

# TABLE INSERTS
dim_time_table_insert = ("""
    INSERT INTO dim_time(
        timestamp,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT
        timestamp,
        EXTRACT(hour from timestamp) as hour,
        EXTRACT(day FROM timestamp) AS day,
        EXTRACT(week FROM timestamp) AS week,
        EXTRACT(month FROM timestamp) AS month,
        EXTRACT(year FROM timestamp) AS year,
        EXTRACT(isodow FROM timestamp) AS weekday
    FROM staging_scraped_books
    UNION ALL
    SELECT
        DISTINCT TO_TIMESTAMP("Erstellt am", 'DD-MM-YYYY') as timestamp,
        EXTRACT(hour from TO_TIMESTAMP("Erstellt am", 'DD-MM-YYYY')) as hour,
        EXTRACT(day FROM TO_TIMESTAMP("Erstellt am", 'DD-MM-YYYY')) AS day,
        EXTRACT(week FROM TO_TIMESTAMP("Erstellt am", 'DD-MM-YYYY')) AS week,
        EXTRACT(month FROM TO_TIMESTAMP("Erstellt am", 'DD-MM-YYYY')) AS month,
        EXTRACT(year FROM TO_TIMESTAMP("Erstellt am", 'DD-MM-YYYY')) AS year,
        EXTRACT(isodow FROM TO_TIMESTAMP("Erstellt am", 'DD-MM-YYYY')) AS weekday
    FROM stagingn_order_overview;
""")

dim_order_table_insert = ("""
    INSERT INTO dim_order(
        order_id,
        status,
        number_of_packages,
        voucher_code,
        email
    )
    SELECT
        "Auftragsnummer" AS order_id,
        "Auftragsstatus" AS status,
        "Anzahl Pakete" AS number_of_packages,
        "Gutschein" AS voucher_code,
        "Email" AS email
    FROM stagingn_order_overview;
""")

dim_book_table_insert = ("""
    INSERT INTO dim_book(
        book_ean,
        title,
        categories,
        language,
        link,
        rating,
        number_of_ratings,
        author,
        product_form,
        publisher,
        decription,
        original_title,
        width,
        height
    )
    SELECT
        DISTINCT RIGHT(rebuy_ean,13) AS ean,
        rebuy_title AS title,
        rebuy_categories AS categories,
        rebuy_language AS language,
        rebuy_link AS link,
        CASE
            WHEN rebuy_rating = '' THEN NULL
            ELSE rebuy_rating::FLOAT
        END AS rating,
        rebuy_number_of_ratings::INT AS number_of_ratings,
        rebuy_author AS author,
        rebuy_product_form AS product_form,
        rebuy_publisher AS publisher,
        rebuy_description AS description,
        rebuy_original_title AS original_title,
        REPLACE(LEFT(rebuy_width,-3),',','.')::FLOAT AS width,
        REPLACE(LEFT(rebuy_height,-3),',','.')::FLOAT AS height
    FROM staging_scraped_books
    ON CONFLICT (book_ean) DO NOTHING
    ;
""")

fact_rebuy_sales_table_insert = """
    INSERT INTO fact_rebuy_sales(
            order_id,
            order_date,
            book_ean,
            sales_price,
            shipping_cost,
            voucher_discount
    )
    SELECT
        DISTINCT staging_ordered_books."Auftragsnummer" AS order_id,
        TO_TIMESTAMP(stagingn_order_overview."Erstellt am", 'DD-MM-YYYY') as order_date,
        staging_scraped_books.rebuy_ean AS book_ean,
        staging_ordered_books."Preis"::FLOAT AS sales_price,
        COALESCE(REPLACE(LEFT(stagingn_order_overview."Versandkosten Preis",-2),',','.')::FLOAT,0) AS shipping_cost,
        COALESCE(REPLACE(LEFT(stagingn_order_overview."Gutschein Preis", -2),',','.')::FLOAT,0)  AS voucher_discount
    FROM staging_ordered_books
    LEFT JOIN staging_scraped_books ON staging_scraped_books.rebuy_title = staging_ordered_books."Titel"
    LEFT JOIN stagingn_order_overview USING("Auftragsnummer");
"""

fact_scraped_books_table_insert = """
    INSERT INTO fact_scraped_books(
        scraped_timestamp,
        book_ean,
        rebuy_price,
        momox_price,
        rebuy_publication_year,
        momox_publication_year,
        spread,
        momox_status,
        is_arbitrage,
        momox_referece_price,
        momox_warehouse_status,
        momox_demand_rating,
        rebuy_condition,
        rebuy_availability
    )
    SELECT
        timestamp,
        rebuy_ean,
        rebuy_price,
        momox_price,
        rebuy_publication_year,
        momox_publication_year,
        spread::FLOAT,
        momox_status,
        abitrage::INTEGER,
        momox_reference_price::FLOAT,
        momox_warehouse_status::INTEGER,
        momox_demand_rating::INTEGER,
        rebuy_condition,
        rebuy_availability
    FROM staging_scraped_books
    ON CONFLICT (scraped_timestamp) DO NOTHING;
"""

# QUERY LISTS
create_table_queries = [
    fact_scraped_books_table_create,
    fact_rebuy_sales_table_create,
    dim_book_table_create,
    dim_order_table_create,
    dim_time_table_create,
]

drop_table_queries = [
    fact_scraped_books_table_drop,
    fact_rebuy_sales_table_drop,
    dim_book_table_drop,
    dim_order_table_drop,
    dim_time_table_drop,
]

insert_table_queries = [
    dim_time_table_insert,
    dim_order_table_insert,
    dim_book_table_insert,
    fact_rebuy_sales_table_insert,
    fact_scraped_books_table_insert
]
