/*
  -- Dave Skura, 2022

	CREATE USERS

*/
DROP USER IF EXISTS larry;
CREATE USER larry WITH PASSWORD 'larry';
GRANT sales_users TO larry;

DROP USER IF EXISTS tomas;
CREATE USER tomas WITH PASSWORD 'tomas';
GRANT accounting_users TO tomas;

DROP USER IF EXISTS dad;
CREATE USER dad WITH PASSWORD 'dad';
GRANT marketing_users TO dad;
GRANT external_users TO dad;

DROP USER IF EXISTS boss;
CREATE USER boss WITH PASSWORD 'boss';
GRANT boss_user TO boss;
