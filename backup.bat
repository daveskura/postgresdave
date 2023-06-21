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

REM ---------------------------------------------------
REM ---------------------------------------------------


FOR /f "tokens=2 delims==" %%G in ('wmic os get localdatetime /value') do set datetime=%%G

REM Building a timestamp from variables
SET "dd=%datetime:~6,2%"
SET "mth=%datetime:~4,2%"
SET "yyyy=%datetime:~0,4%"
SET "Date=%yyyy%%mth%%dd%"

FOR /f "tokens=2 delims==" %%G in ('wmic os get localdatetime /value') do set datetime=%%G

REM Variable format 1
SET TMS=%time:~0,2%-%time:~3,2%-%time:~6,2%

REM echo backing up schema %DB_SCHEMA% on %DB_TO_BACKUP% database hosted on %DB_HOST%:%DB_PORT%
echo backing up %DB_TO_BACKUP% database hosted on %DB_HOST%:%DB_PORT%

REM %WHERE_IS_PG_DUMP%\pg_dump -U %DB_USER% -n %DB_SCHEMA% -d %DB_TO_BACKUP% -h %DB_HOST% > %DB_TO_BACKUP%_%DB_SCHEMA%_%Date%_%TMS%.sql
%WHERE_IS_PG_DUMP%\pg_dump -U %DB_USER% -d %DB_TO_BACKUP% -h %DB_HOST% > %DB_TO_BACKUP%_%Date%_%TMS%.sql