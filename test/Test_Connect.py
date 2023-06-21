"""
  Dave Skura
  
"""
from postgresdave_package.postgresdave import postgres_db 

import logging
logging.basicConfig(level=logging.INFO)
logging.info(" Starting ") # 
postgres = postgres_db()

postgres.connect()
postgres.close()