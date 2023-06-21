
#  find the blocked pid of ALTER TABLE

	SELECT pid, state, backend_start, substr(query, 0, 100) q
	FROM pg_stat_activity
	WHERE backend_type = 'client backend' AND query LIKE 'ALTER TABLE%'
	ORDER BY backend_start;

# Connections Open Longer Than 10 Seconds

	SELECT 'SELECT pg_terminate_backend(' || pid || ');'
	FROM pg_stat_activity
	WHERE backend_type = 'client backend'
	      AND pid != pg_backend_pid()
	      AND backend_start < NOW() - '10 seconds'::interval;


# query role details

	SELECT r.rolname, r.rolsuper, r.rolinherit,
	  r.rolcreaterole, r.rolcreatedb, r.rolcanlogin,
	  r.rolconnlimit, r.rolvaliduntil,
	  ARRAY(SELECT b.rolname
		FROM pg_catalog.pg_auth_members m
		JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)
		WHERE m.member = r.oid) as memberof
	, r.rolreplication
	, r.rolbypassrls
	FROM pg_catalog.pg_roles r
	WHERE r.rolname !~ '^pg_'
	ORDER BY 1;

	rolname	rolsuper	rolinherit	rolcreaterole	rolcreatedb	rolcanlogin	rolconnlimit	rolvaliduntil	memberof	rolreplication	rolbypassrls
	dad	true	true	true	true	true	-1	(null)	{}	true	false
	larry	false	true	false	false	true	-1	(null)	{"users_sales"}	false	false
	postgres	true	true	true	true	true	-1	(null)	{}	true	true
	tomas	false	true	false	false	true	-1	(null)	{"users_accounting"}	false	false
	users_accounting	false	true	false	false	false	-1	(null)	{}	false	false
	users_sales	false	true	false	false	false	-1	(null)	{}	false	false
