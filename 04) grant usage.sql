/*
  -- Dave Skura, 2022
*/

REVOKE ALL ON SCHEMA public FROM public;

GRANT ALL ON SCHEMA _accounting TO accounting_users;
GRANT ALL ON SCHEMA _sales TO sales_users;
GRANT ALL ON SCHEMA _marketing TO marketing_users;
GRANT ALL ON SCHEMA _external TO external_users;


GRANT SELECT ON ALL TABLES IN SCHEMA "_accounting" TO boss_user;
GRANT SELECT ON ALL TABLES IN SCHEMA "_sales" TO boss_user;
GRANT SELECT ON ALL TABLES IN SCHEMA "_marketing" TO boss_user;
GRANT SELECT ON ALL TABLES IN SCHEMA "_external" TO boss_user;