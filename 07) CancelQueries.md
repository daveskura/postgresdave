# Stopping Queries Through the Operating System

## on unix
use a tool like htop to check the running backend processes. Backend processes appear as children of the main PostgreSQL server process.

	kill -SIGTERM pid
	kill -KILL pid

## on windows

	pg_ctl kill TERM pid
	pg_ctl kill INT pid
	pg_ctl kill KILL pid


# in SQL

## Find pid

	SELECT pid, state, backend_start, substr(query, 0, 100) q
	FROM pg_stat_activity
	WHERE backend_type = 'client backend'
	ORDER BY backend_start;

## Terminate, or Cancel, the Process

	SELECT pg_terminate_backend(pid); -- this one uses force.  rolls back any open transaction,connection to closed
	SELECT pg_cancel_backend(pid); --   saves transaction,connection still open

## generate SQL to terminate all running queries that look like they’re using a particlar table called auth_user:

	SELECT 'SELECT pg_terminate_backend(' || pid || ');'
	FROM pg_stat_activity
	WHERE query LIKE '%auth_user%'
	  AND pid != pg_backend_pid();

### Reference Link
https://adamj.eu/tech/2022/06/20/how-to-find-and-stop-running-queries-on-postgresql/
