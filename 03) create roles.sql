/*
  -- Dave Skura, 2022
*/
DROP ROLE IF EXISTS accounting_users;
DROP ROLE IF EXISTS sales_users;
DROP ROLE IF EXISTS marketing_users;
DROP ROLE IF EXISTS external_users;
DROP ROLE IF EXISTS boss_user;

CREATE ROLE accounting_users;
CREATE ROLE sales_users;
CREATE ROLE marketing_users;
CREATE ROLE external_users;

CREATE ROLE boss_user;
