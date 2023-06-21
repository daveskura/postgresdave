"""
  Dave Skura
  
"""
from sqlitedave_package.sqlitedave import sqlite_db 

import logging
logging.basicConfig(level=logging.INFO)
logging.info(" Starting ") # 
sqlite = sqlite_db()

sqlite.connect()
sqlite.close()
