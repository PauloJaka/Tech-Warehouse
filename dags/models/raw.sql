CREATE TABLE raw (
    id SERIAL PRIMARY KEY,
    title TEXT,
    discount_price NUMERIC(7, 2),
    original_price NUMERIC(7, 2),
    brand varchar(20),
    rating NUMERIC(3, 2),
    link TEXT,
    free_freight BOOLEAN,
    category varchar(20),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    website varchar(15)
);