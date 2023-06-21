@echo off
REM ---------------------------------------------------
REM
REM  Dave Skura, 2022
REM
REM ---------------------------------------------------
REM ---------------------------------------------------

SET WHERE_IS_PG_DUMP=c:\postgres_database\bin
SET DB_HOST=localhost
SET DB_TO_BACKUP=bq_db
SET DB_USER=postgres
SET PGPASSWORD=4165605869
REM SET DB_SCHEMA=_keydemo

SET BACKUPFILE=bq_db__keydemo_20230124_11-08-13.sql

REM ---------------------------------------------------
REM ---------------------------------------------------

echo restoring %DB_TO_BACKUP% database hosted on %DB_HOST%:%DB_PORT%

%WHERE_IS_PG_DUMP%\psql -U %DB_USER% -d %DB_TO_BACKUP% -h %DB_HOST%  < %BACKUPFILE%

