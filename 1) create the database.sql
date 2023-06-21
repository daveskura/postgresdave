/*
  -- Dave Skura, 2022

	CREATE the database

*/


-- Database: company_db
-- DROP DATABASE IF EXISTS company_db;

CREATE DATABASE company_db
	WITH
	OWNER = postgres
	TABLESPACE = pg_default
	CONNECTION LIMIT = -1
	IS_TEMPLATE = False;
