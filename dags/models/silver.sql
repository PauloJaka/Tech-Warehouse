create table lakehouse.f_silver (
	id int primary key,
	created_at TIMESTAMP,
    updated_at TIMESTAMP,
    ingestion_at TIMESTAMP,
    website varchar(15),
    category varchar(20)
);

create table lakehouse.d_silver_notebooks(
    id int, 
    title TEXT,
    discount_price NUMERIC(7, 2),
    original_price NUMERIC(7, 2),
    brand varchar(20),
    rating NUMERIC(3, 2),
    link TEXT,
    free_freight BOOLEAN,
    model varchar(40),
    CPU varchar(40),
    GPU varchar(40),
    RAM varchar(5),
    SSD varchar(6)
);

create table lakehouse.d_silver_smartphone (
    id int, 
    title TEXT,
    discount_price NUMERIC(7, 2),
    original_price NUMERIC(7, 2),
    brand varchar(20),
    rating NUMERIC(3, 2),
    link TEXT,
    model varchar(30),
    dual_sim bool,
    RAM varchar(5),
    storage_capacity varchar(6)
)

create table lakehouse.d_silver_tv (
    id int, 
    title TEXT,
    discount_price NUMERIC(7, 2),
    original_price NUMERIC(7, 2),
    brand varchar(20),
    rating NUMERIC(3, 2),
    link TEXT,
    model varchar(30),
	size varchar(4),
	resolution varchar(25),
	technology varchar(15)
);

create table lakehouse.d_silver_tablets(
    id int, 
    title TEXT,
    discount_price NUMERIC(7, 2),
    original_price NUMERIC(7, 2),
    brand varchar(20),
    rating NUMERIC(3, 2),
    link TEXT,
    model varchar(30),
	RAM varchar(5),
    storage_capacity varchar(6)
);

create table lakehouse.d_silver_smartwatch(
    id int, 
    title TEXT,
    discount_price NUMERIC(7, 2),
    original_price NUMERIC(7, 2),
    brand varchar(20),
    rating NUMERIC(3, 2),
    link TEXT,
    model varchar(30)
);

ALTER TABLE lakehouse.d_silver_tv 
ADD CONSTRAINT fk_silver FOREIGN KEY (id) REFERENCES lakehouse.f_silver(id);

ALTER TABLE lakehouse.d_silver_notebooks
ADD CONSTRAINT fk_silver FOREIGN KEY (id) REFERENCES lakehouse.f_silver(id);

ALTER TABLE lakehouse.d_silver_smartwatch
ADD CONSTRAINT fk_silver FOREIGN KEY (id) REFERENCES lakehouse.f_silver(id);

ALTER TABLE lakehouse.d_silver_smartphone
ADD CONSTRAINT fk_silver FOREIGN KEY (id) REFERENCES lakehouse.f_silver(id);

