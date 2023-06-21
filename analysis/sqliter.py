"""
  Dave Skura
  
"""
import logging
import sys
from sqlitedave_package.sqlitedave import sqlite_db 

sqlite = sqlite_db()
sqlite.connect()

		
if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	sql = ''

	if len(sys.argv) == 1 or sys.argv[1] == 'sqliter.py': # no parameters
		logging.info('usage: \n')
		logging.info('sqliter.py -f sqlfilename ') 
		logging.info('sqliter.py <SELECT SQL Statement>')
		logging.info('sqliter.py "SELECT name FROM sqlite_master WHERE type = \'table\'"')
		logging.info('sqliter.py "SELECT * FROM postgres_table_counts ORDER BY row_count DESC limit 10"')
		print('\nTables:\n')
		print(sqlite.export_query_to_str("SELECT name FROM sqlite_master WHERE type = 'table'",'\t'))
		sqlite.close()
		sys.exit(0)

	elif (sys.argv[1].lower() == '-f'):
		sqlfile = sys.argv[2]
		logging.info('opening sql file: ' + sqlfile + '\n')
		f = open(sqlfile,'r')
		sql = f.read()
		logging.info('Running query: ' + sql + '\n\n')
	else:
		sql = sys.argv[1]
		logging.info('Running query: ' + sql + '\n\n')
	

print(sqlite.export_query_to_str(sql,'\t'))

sqlite.close()


