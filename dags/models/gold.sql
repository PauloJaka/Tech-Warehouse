create table lakehouse.f_gold (
	id int primary key,
	created_at TIMESTAMP,
    updated_at TIMESTAMP,
    ingestion_at TIMESTAMP,
    website varchar(15),
    category varchar(20)
);


create table lakehouse.d_gold_notebooks(
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
    SSD varchar(6),
    specifics varchar(7)
);

create table lakehouse.d_gold_smartphone (
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
    storage_capacity varchar(6),
    free_freight BOOLEAN,
    specifics varchar(6)
);

create table lakehouse.d_gold_tv (
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
	technology varchar(15),
    free_freight BOOLEAN,
    specifics varchar(15)
);

create table lakehouse.d_gold_tablets(
    id int, 
    title TEXT,
    discount_price NUMERIC(7, 2),
    original_price NUMERIC(7, 2),
    brand varchar(20),
    rating NUMERIC(3, 2),
    link TEXT,
    model varchar(30),
	RAM varchar(5),
    storage_capacity varchar(6),
    free_freight BOOLEAN,
    specifics varchar(6)
);

create table lakehouse.d_gold_smartwatch(
    id int, 
    title TEXT,
    discount_price NUMERIC(7, 2),
    original_price NUMERIC(7, 2),
    brand varchar(20),
    rating NUMERIC(3, 2),
    link TEXT,
    model varchar(30),
    free_freight BOOLEAN,
    specifics varchar(12)
);


ALTER TABLE lakehouse.d_gold_tv 
ADD CONSTRAINT fk_gold FOREIGN KEY (id) REFERENCES lakehouse.f_gold(id);

ALTER TABLE lakehouse.d_gold_notebooks
ADD CONSTRAINT fk_gold FOREIGN KEY (id) REFERENCES lakehouse.f_gold(id);

ALTER TABLE lakehouse.d_gold_smartwatch
ADD CONSTRAINT fk_gold FOREIGN KEY (id) REFERENCES lakehouse.f_gold(id);

ALTER TABLE lakehouse.d_gold_smartphone
ADD CONSTRAINT fk_gold FOREIGN KEY (id) REFERENCES lakehouse.f_gold(id);