/*
  -- Dave Skura, 2022
  login as postgres
  create role
  assign user to role 
  grant permissions
  create tables

  ** if managing permissions by schema, revoke the default access to public schema
  REVOKE ALL ON SCHEMA public FROM public;
*/


DROP ROLE IF EXISTS keydemo_users;
CREATE ROLE keydemo_users;

GRANT keydemo_users TO dave;

DROP SCHEMA IF EXISTS _keydemo;
CREATE SCHEMA _keydemo;

-- GRANT SELECT ON ALL TABLES IN SCHEMA "_keydemo" TO keydemo_users;
GRANT ALL ON SCHEMA _keydemo TO keydemo_users;

-- ***** LOGOUT postgres

-- ***** LOGIN as dave

DROP TABLE IF EXISTS _keydemo.products;
CREATE TABLE _keydemo.products (
	product_id int primary key,
	product_name varchar(250)
);


DROP TABLE IF EXISTS _keydemo.customer;
CREATE TABLE _keydemo.customer (
	customer_id int primary key,
	customer_name varchar(250)
);

DROP TABLE IF EXISTS _keydemo.sales;
CREATE TABLE _keydemo.sales (
	invoice_nbr	int ,
	line_nbr int ,
	product_id int references _keydemo.products(product_id),
	customer_id int references _keydemo.customer(customer_id),
	saleamt numeric(8,3),
	primary key(invoice_nbr,line_nbr)
);

INSERT INTO _keydemo.products(product_id,product_name) VALUES(1001,'hat');
INSERT INTO _keydemo.products(product_id,product_name) VALUES(1002,'gloves');
INSERT INTO _keydemo.products(product_id,product_name) VALUES(1003,'shoes');


INSERT INTO _keydemo.customer(customer_id,customer_name) VALUES(701,'Larry');
INSERT INTO _keydemo.customer(customer_id,customer_name) VALUES(702,'Curly');
INSERT INTO _keydemo.customer(customer_id,customer_name) VALUES(703,'Moe');


INSERT INTO _keydemo.sales(invoice_nbr,line_nbr,product_id,customer_id,saleamt) VALUES(20020212,1,1001,701,0.97);
INSERT INTO _keydemo.sales(invoice_nbr,line_nbr,product_id,customer_id,saleamt) VALUES(20020212,2,1002,701,2.99);
INSERT INTO _keydemo.sales(invoice_nbr,line_nbr,product_id,customer_id,saleamt) VALUES(20020212,3,1003,701,12.01);

SELECT invoice_nbr,customer_name,product_name,saleamt
FROM _keydemo.sales 
	INNER JOIN _keydemo.products USING (product_id)
	INNER JOIN _keydemo.customer USING (customer_id);


INSERT INTO _keydemo.products(product_id,product_name) VALUES(1001,'fail');
INSERT INTO _keydemo.customer(customer_id,customer_name) VALUES(701,'fail');
INSERT INTO _keydemo.sales(invoice_nbr,line_nbr,product_id,customer_id,saleamt) VALUES(20020212,1,1,	701,-1);
INSERT INTO _keydemo.sales(invoice_nbr,line_nbr,product_id,customer_id,saleamt) VALUES(20020212,1,1001,	1,	-1);







