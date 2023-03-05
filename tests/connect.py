"""
  Dave Skura
  
"""
from postgresdave_package.postgresdave import postgres_db 

mydb = postgres_db()
mydb.connect()
mydb.close()	

