### dbversion()
returns Postgres version

###  __init__(DB_USERPWD,DB_SCHEMA)
constructor.  DB_USERPWD & DB_SCHEMA are optional

Connection details are all defaulted:
DatabaseType='Postgres' 
DB_USERNAME='postgres' 
DB_HOST='localhost' 
DB_PORT='1532' 
DB_NAME='postgres' 
DB_SCHEMA='public'		

### savepwd(pwd)
saved password locally so you dont have to pass it in next time

### saveConnectionDefaults(DB_USERNAME,DB_USERPWD,DB_HOST,DB_PORT,DB_NAME,DB_SCHEMA)
save all connection details locally

### useConnectionDetails(DB_USERNAME,DB_USERPWD,DB_HOST,DB_PORT,DB_NAME,DB_SCHEMA)
Use these connection details and connect.  

### is_an_int(prm)
utility.  check if value it an int

### export_query_to_str(qry,szdelimiter=',')
Run query.
Return results in a string formatted like a table

### export_query_to_csv(qry,csv_filename,szdelimiter=',')
Run query.
Return save results in file 

### export_table_to_csv(csvfile,tblname,szdelimiter=',')
Read Table.
Return results in a file 

### load_csv_to_table_orig(csvfile,tblname,withtruncate=True,szdelimiter=',')
Load a table from a csv file

### does_table_exist(tblname)
utility check if table exists

### close()
close database connection

### connect(self)
Connect to the database

### execute(qry):
Connect to the database and run the query.

### query(qry)
Connect to the database and run the query.
Return all data from query

### commit()
database commit

### commandline
connect & version check


