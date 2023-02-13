"""
  Dave Skura
"""
import os

from postgresdave import db 

print (" tester ") # 
print('')

mydb = db()
mydb.connect()
print(mydb.dbversion())

mydb.close()	
print('')

