"""
  Dave Skura

	Connect to Postgres
	Connect to local sqlite_db
	read tables and rowcounts by schema/table
	load into sqlite_db

"""
from sqlitedave_package.sqlitedave import sqlite_db 
from postgresdave_package.postgresdave import postgres_db 
from schemawizard_package.schemawizard import schemawiz

import logging

class runner():
	def __init__(self):
		self.sqlite = sqlite_db()
		self.postgres = postgres_db()
		self.connect()
		query_tablecounts = """
			select table_schema, 
						 table_name, 
						 (xpath('/row/cnt/text()', xml_count))[1]::text::int as row_count
			from (
				select table_name, table_schema, 
							 query_to_xml(format('select count(*) as cnt from %I.%I', table_schema, table_name), false, true, '') as xml_count
				from information_schema.tables
				where table_schema not in ('pg_catalog','information_schema')
			) t;
		"""
		query_schemacounts = """

			select table_schema, count(*) as table_count
			from information_schema.tables
			where table_schema not in ('pg_catalog','information_schema')
			group by table_schema


		"""
    
		tblcountsname = 'postgres_table_counts'
		schemacountsname = 'postgres_schemas'


		csvtablefilename = 'tables.tsv'
		csvschemafilename = 'schemas.tsv'
		logging.info("Querying Postgres for schema counts using query_to_xml/xpath against (information_schema.tables) ") # 
		self.postgres.export_query_to_csv(query_schemacounts,csvschemafilename,'\t')

		logging.info("Querying Postgres for table counts using query_to_xml/xpath against (information_schema.tables) ") # 
		self.postgres.export_query_to_csv(query_tablecounts,csvtablefilename,'\t')

		logging.info("Loading " + csvschemafilename + ' to local_sqlite_db') # 

		if self.sqlite.does_table_exist(schemacountsname):
			logging.info('table ' + schemacountsname + ' exists.')
			logging.info('tuncate/load table ' + schemacountsname)
			self.sqlite.load_csv_to_table(csvschemafilename,schemacountsname,True,'\t')
		else:
			obj = schemawiz(csvschemafilename)
			sqlite_ddl = obj.guess_sqlite_ddl(schemacountsname)

			logging.info('\nCreating ' + schemacountsname)
			self.sqlite.execute(sqlite_ddl)

			self.sqlite.load_csv_to_table(csvschemafilename,schemacountsname,False,obj.delimiter)

		logging.info(schemacountsname + ' has ' + str(self.sqlite.queryone('SELECT COUNT(*) FROM ' + schemacountsname)) + ' rows.\n') 


		logging.info("Loading " + csvtablefilename + ' to local_sqlite_db') # 

		if self.sqlite.does_table_exist(tblcountsname):
			logging.info('table ' + tblcountsname + ' exists.')
			logging.info('tuncate/load table ' + tblcountsname)
			self.sqlite.load_csv_to_table(csvtablefilename,tblcountsname,True,'\t')
		else:
			obj = schemawiz(csvtablefilename)
			sqlite_ddl = obj.guess_sqlite_ddl(tblcountsname)

			logging.info('\nCreating ' + tblcountsname)
			self.sqlite.execute(sqlite_ddl)

			self.sqlite.load_csv_to_table(csvtablefilename,tblcountsname,False,obj.delimiter)

		logging.info(tblcountsname + ' has ' + str(self.sqlite.queryone('SELECT COUNT(*) FROM ' + tblcountsname)) + ' rows.\n') 



		self.disconnect()
	def connect(self):
		self.postgres.connect()
		logging.info('Connected to ' + self.postgres.db_conn_dets.dbconnectionstr())
		self.sqlite.connect()
		logging.info('Connected to ' + self.sqlite.db_conn_dets.dbconnectionstr())

	def disconnect(self):
		self.sqlite.close()
		self.postgres.close()

if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	logging.info(" Starting Simple Analysis") # 

	runner()
