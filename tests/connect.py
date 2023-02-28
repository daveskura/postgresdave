"""
  Dave Skura
  
"""
from postgresdave_package.postgresdave import db 

mydb = db()
mydb.connect()
mydb.close()	

