# Quick Start Guide

### test your database connection on the commandline
py -m postgresdave_package.postgresdave

You will be prompted for a password if there is not one saved locally.  You can type in the password when prompted for the test connect and decide to either save the password locally or not using the prompts.

### If your postgres database is installed on your laptop/pc

All the connection details will be defaulted, except password.  You can either enter in the password when prompted or save the password locally once using the method savepwd().  Calling this method once will cause a .pwd file to be created.  After this you don't have to call the savepwd() method again unless the password changes.

---

#### Try just connecting and follow the prompts:
>
> from postgresdave_package.postgresdave import db 
>
> mydb = db()
>
> mydb.connect()
>
> print(mydb.dbversion())
>
> mydb.close()
> 


#### Alternatively, specify one time, the password in code then connect using defaults:
>
> from postgresdave_package.postgresdave import db 
>
> mydb = db()
>
> ***mydb.savepwd('mypassword')***
>
> mydb.connect()
>
> print(mydb.dbversion())
>
> mydb.close()
> 

### If your postgres database is installed with other connection details

Save the connection details locally once using the method saveConnectionDefaults().  Calling this method once will cause a .connection and .pwd file to be created.  After this you don't have to call the saveConnectionDefaults() method again unless the connection details change.

---
>
> from postgresdave_package.postgresdave import db 
>
> mydb = db()
>
> mydb.saveConnectionDefaults(DB_USERNAME,DB_USERPWD,DB_HOST,DB_PORT,DB_NAME,DB_SCHEMA)
>
> mydb.connect()
>
> print(mydb.dbversion())
>
> mydb.close()
> 

### Connect and run your queries using execute(qry),query(qry),queryone(qry),export_query_to_str(qry)

>
> from postgresdave_package.postgresdave import db 
>
> mydb = db()
>
> mydb.connect()
>
> print(mydb.dbversion())
>
> print(' - - - - - - - - - - - - - - - - - - - - - - - - - - -  \n')
>
> print('table_count = ' + str(mydb.queryone('SELECT COUNT(*) as table_count FROM INFORMATION_SCHEMA.TABLES')))
>
> print(' - - - - - - - - - - - - - - - - - - - - - - - - - - -  \n')
> 
> qry = """
>
> SELECT DISTINCT table_catalog as database_name, table_schema as schema 
>
> FROM INFORMATION_SCHEMA.TABLES
>
> """
>
> print(mydb.export_query_to_str(qry,'\t'))
> 
> mydb.close()
> 

### Load a Postgres table from a matching csv file

>
> from postgresdave_package.postgresdave import db 
>
> mydb = db()
>
> mydb.connect()
>
> mydb.load_csv_to_table('customer.csv','customer',True,'\t') # truncate first=True, delimiter=tab
>
> print('Table loaded')
>
> mydb.close()
>

### Export a Postgres table to a csv file

>
> from postgresdave_package.postgresdave import db 
>
> mydb = db()
>
> mydb.connect()
>
> mydb.export_table_to_csv('customer.csv','customer','\t') # delimiter=tab
>
> print('csv file created ')
>
> mydb.close()
>
