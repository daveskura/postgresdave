"""
  Dave Skura
"""
import os

print (" tester ") # 
from postgresdave import db 

mydb = db()
print(mydb.dbversion())
