create table lakehouse.f_bronze (
	id int primary key,
	created_at TIMESTAMP,
    updated_at TIMESTAMP,
    website varchar(15),
    category varchar(20)
);

create table lakehouse.d_bronze_tv(
    id int, 
    title TEXT,
    discount_price NUMERIC(7, 2),
    original_price NUMERIC(7, 2),
    brand varchar(20),
    rating NUMERIC(3, 2),
    link TEXT,
    free_freight BOOLEAN
);

create table lakehouse.d_bronze_smartphone(
    id int, 
    title TEXT,
    discount_price NUMERIC(7, 2),
    original_price NUMERIC(7, 2),
    brand varchar(20),
    rating NUMERIC(3, 2),
    link TEXT,
    free_freight BOOLEAN
);

create table lakehouse.d_bronze_notebooks(
    id int, 
    title TEXT,
    discount_price NUMERIC(7, 2),
    original_price NUMERIC(7, 2),
    brand varchar(20),
    rating NUMERIC(3, 2),
    link TEXT,
    free_freight BOOLEAN
);

create table lakehouse.d_bronze_smartwatch(
    id int, 
    title TEXT,
    discount_price NUMERIC(7, 2),
    original_price NUMERIC(7, 2),
    brand varchar(20),
    rating NUMERIC(3, 2),
    link TEXT,
    free_freight BOOLEAN
);

create table lakehouse.d_bronze_tablets(
    id int, 
    title TEXT,
    discount_price NUMERIC(7, 2),
    original_price NUMERIC(7, 2),
    brand varchar(20),
    rating NUMERIC(3, 2),
    link TEXT,
    free_freight BOOLEAN
);

ALTER TABLE lakehouse.d_bronze_tv 
ADD CONSTRAINT fk_bronze FOREIGN KEY (id) REFERENCES lakehouse.f_bronze(id);

ALTER TABLE lakehouse.d_bronze_notebooks 
ADD CONSTRAINT fk_bronze FOREIGN KEY (id) REFERENCES lakehouse.f_bronze(id);

ALTER TABLE lakehouse.d_bronze_smartwatch
ADD CONSTRAINT fk_bronze FOREIGN KEY (id) REFERENCES lakehouse.f_bronze(id);

ALTER TABLE lakehouse.d_bronze_smartphone
ADD CONSTRAINT fk_bronze FOREIGN KEY (id) REFERENCES lakehouse.f_bronze(id);