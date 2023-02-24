# Quick Start Guide

### test your database connection on the commandline
py -m postgresdave_package.postgresdave

You will be prompted for a password if there is not one saved locally.  You can type in the password when prompted for the test connect and decide to either save the password locally or not using the prompts.

### If your postgres database is installed on your laptop/pc

All the connection details will be defaulted, except password.  You can either enter in the password when prompted or save the password locally once using the method savepwd().  Calling this method once will cause a .pwd file to be created.  After this you don't have to call the savepwd() method again unless the password changes.

---

Try just connecting and follow the prompts:
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

Specify one time, the password in code then connect using defaults:
>
> from postgresdave_package.postgresdave import db 
>
> mydb = db()
>
> mydb.savepwd('mypassword')
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

