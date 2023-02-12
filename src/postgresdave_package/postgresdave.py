"""
  Dave Skura, 2023
"""
import os
import sys
import psycopg2 

class db_defaults: 
	def __init__(self): 
		self.DatabaseType='Postgres' 
		self.updated='Dec 2/2023' 
		 
		self.DB_USERNAME='postgres' 
		self.DB_HOST='localhost' 
		self.DB_PORT='1532' 
		self.DB_NAME='postgres' 
		self.DB_SCHEMA=''		#'weather' 
		self.dbconnectionstr='usr=postgres; svr=localhost; port=1532; schema=postgres;' 

class db:
	def dbversion(self):
		return self.queryone('SELECT VERSION()')

	def __init__(self,DB_USERPWD='no-password-supplied',DB_SCHEMA='public'):
		self.version=1.0

		self.db_dets = db_defaults()
		self.db_dets.DB_SCHEMA = DB_SCHEMA

		self.ihost = self.db_dets.DB_HOST				
		self.iport = self.db_dets.DB_PORT				
		self.idb = self.db_dets.DB_NAME					
		self.ischema = self.db_dets.DB_SCHEMA		
		if self.ischema == '':
			self.ischema = 'public'
		self.iuser = self.db_dets.DB_USERNAME		
		self.ipwd = DB_USERPWD			
		self.connection_str = self.db_dets.dbconnectionstr

		self.dbconn = None
		self.cur = None
		if DB_USERPWD != 'no-password-supplied':
			self.connect()
		else:
			try:
				f = open('.pwd','r')
				PWD_in_file = f.read()
				self.ipwd = PWD_in_file
				f.close()
				self.connect()
			except: 
				pass

	def setConnectionDetails(self,DB_USERNAME,DB_USERPWD,DB_HOST,DB_PORT,DB_NAME,DB_SCHEMA):

		self.iuser = DB_USERNAME
		self.ipwd = DB_USERPWD			
		self.ihost = DB_HOST				
		self.iport = DB_PORT				
		self.idb = DB_NAME					
		self.ischema = DB_SCHEMA		
		if self.ischema == '':
			self.ischema = 'public'
		self.connect()

	def is_an_int(self,prm):
			try:
				if int(prm) == int(prm):
					return True
				else:
					return False
			except:
					return False

	def export_query_to_str(self,qry,szdelimiter=','):
		self.cur.execute(qry)
		f = ''
		sz = ''
		for k in [i[0] for i in self.cur.description]:
			sz += k + szdelimiter
		f += sz[:-1] + '\n'

		for row in self.cur:
			sz = ''
			for i in range(0,len(self.cur.description)):
				sz += str(row[i])+ szdelimiter

			f += sz[:-1] + '\n'

		return f

	def export_query_to_csv(self,qry,csv_filename,szdelimiter=','):
		self.cur.execute(qry)
		f = open(csv_filename,'w')
		sz = ''
		for k in [i[0] for i in self.cur.description]:
			sz += k + szdelimiter
		f.write(sz[:-1] + '\n')

		for row in self.cur:
			sz = ''
			for i in range(0,len(self.cur.description)):
				sz += str(row[i])+ szdelimiter

			f.write(sz[:-1] + '\n')
				

	def export_table_to_csv(self,csvfile,tblname,szdelimiter=','):
		if not self.does_table_exist(tblname):
			raise Exception('Table does not exist.  Create table first')

		this_schema = tblname.split('.')[0]
		try:
			this_table = tblname.split('.')[1]
		except:
			this_schema = self.ischema
			this_table = tblname.split('.')[0]

		qualified_table = this_schema + '.' + this_table

		self.export_query_to_csv('SELECT * FROM ' + qualified_table,csvfile,szdelimiter)

	def load_csv_to_table(self,csvfile,tblname,withtruncate=True,szdelimiter=',',fields='',withextrafields={}):
		this_schema = tblname.split('.')[0]
		try:
			this_table = tblname.split('.')[1]
		except:
			this_schema = self.ischema
			this_table = tblname.split('.')[0]

		qualified_table = this_schema + '.' + this_table

		if not self.does_table_exist(tblname):
			raise Exception('Table does not exist.  Create table first')

		if withtruncate:
			self.execute('TRUNCATE TABLE ' + qualified_table)

		f = open(csvfile,'r')
		hdrs = f.read(1000).split('\n')[0].strip().split(szdelimiter)
		f.close()		

		isqlhdr = 'INSERT INTO ' + qualified_table + '('

		if fields != '':
			isqlhdr += fields	+ ') VALUES '	
		else:
			for i in range(0,len(hdrs)):
				isqlhdr += hdrs[i] + ','
			isqlhdr = isqlhdr[:-1] + ') VALUES '

		skiprow1 = 0
		batchcount = 0
		ilines = ''

		with open(csvfile) as myfile:
			for line in myfile:
				if line.strip()!='':
					if skiprow1 == 0:
						skiprow1 = 1
					else:
						batchcount += 1
						row = line.rstrip("\n").split(szdelimiter)
						newline = "("
						for var in withextrafields:
							newline += "'" + withextrafields[var]  + "',"

						for j in range(0,len(row)):
							if row[j].lower() == 'none' or row[j].lower() == 'null':
								newline += "NULL,"
							else:
								newline += "'" + row[j].replace(',','').replace("'",'').replace('"','') + "',"
							
						ilines += newline[:-1] + '),'
						
						if batchcount > 500:
							qry = isqlhdr + ilines[:-1]
							batchcount = 0
							ilines = ''
							self.execute(qry)

		if batchcount > 0:
			qry = isqlhdr + ilines[:-1]
			batchcount = 0
			ilines = ''
			self.execute(qry)


	def load_csv_to_table_orig(self,csvfile,tblname,withtruncate=True,szdelimiter=','):
		this_schema = tblname.split('.')[0]
		try:
			this_table = tblname.split('.')[1]
		except:
			this_schema = self.ischema
			this_table = tblname.split('.')[0]

		qualified_table = this_schema + '.' + this_table

		if not self.does_table_exist(tblname):
			raise Exception('Table does not exist.  Create table first')

		if withtruncate:
			self.execute('TRUNCATE TABLE ' + qualified_table)

		f = open(csvfile,'r')
		hdrs = f.read(1000).split('\n')[0].strip().split(szdelimiter)
		f.close()		

		isqlhdr = 'INSERT INTO ' + qualified_table + '('

		for i in range(0,len(hdrs)):
			isqlhdr += hdrs[i] + ','
		isqlhdr = isqlhdr[:-1] + ') VALUES '

		skiprow1 = 0
		batchcount = 0
		ilines = ''

		with open(csvfile) as myfile:
			for line in myfile:
				if skiprow1 == 0:
					skiprow1 = 1
				else:
					batchcount += 1
					row = line.rstrip("\n").split(szdelimiter)

					newline = '('
					for j in range(0,len(row)):
						if row[j].lower() == 'none' or row[j].lower() == 'null':
							newline += "NULL,"
						else:
							newline += "'" + row[j].replace(',','').replace("'",'') + "',"
						
					ilines += newline[:-1] + '),'
					
					if batchcount > 500:
						qry = isqlhdr + ilines[:-1]
						batchcount = 0
						ilines = ''
						self.execute(qry)

		if batchcount > 0:
			qry = isqlhdr + ilines[:-1]
			batchcount = 0
			ilines = ''
			self.execute(qry)


	def does_table_exist(self,tblname):
		# tblname may have a schema prefix like public.sales
		#		or not... like sales

		try:
			this_schema = tblname.split('.')[0]
			this_table = tblname.split('.')[1]
		except:
			this_schema = self.ischema
			this_table = tblname.split('.')[0]

		sql = """
			SELECT count(*)  
			FROM information_schema.tables
			WHERE table_schema = '""" + this_schema + """' and table_name='""" + this_table + "'"
		
		if self.queryone(sql) == 0:
			return False
		else:
			return True

	def close(self):
		if self.dbconn:
			self.dbconn.close()

	def connect(self):
		p_options = "-c search_path=" + self.ischema
		try:
			if not self.dbconn:
				self.dbconn = psycopg2.connect(
						host=self.ihost,
						database=self.idb,
						user=self.iuser,
						password=self.ipwd,
						options=p_options
				)
				self.dbconn.set_session(autocommit=True)
				self.cur = self.dbconn.cursor()
		except Exception as e:
			raise Exception(str(e))

	def query(self,qry):
		if not self.dbconn:
			self.connect()

		self.cur.execute(qry)
		all_rows_of_data = self.cur.fetchall()
		return all_rows_of_data

	def commit(self):
		self.dbconn.commit()

	def close(self):
		self.dbconn.close()

	def execute(self,qry):
		try:
			if not self.dbconn:
				self.connect()
			self.cur.execute(qry)
		except Exception as e:
			raise Exception("SQL ERROR:\n\n" + str(e))

	def queryone(self,select_one_fld):
		try:
			if not self.dbconn:
				self.connect()
			self.execute(select_one_fld)
			retval=self.cur.fetchone()
			return retval[0]
		except Exception as e:
			raise Exception("SQL ERROR:\n\n" + str(e))



