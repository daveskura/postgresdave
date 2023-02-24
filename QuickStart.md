# Quick Start Guide

### If your postgres database is installed on your laptop/pc

All the connection details will be defaulted, except password.  Save the password locally once using the method savepwd().

---
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