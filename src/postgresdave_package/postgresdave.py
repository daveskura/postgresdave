"""
  Dave Skura, 2023
"""
import os
import sys
import psycopg2 

class dbconnection_details: 
	def __init__(self): 
		self.DatabaseType='Postgres' 
		self.updated='Feb 13/2023' 

		self.DB_USERNAME='' 
		self.DB_USERPWD=''
		self.DB_HOST='' 
		self.DB_PORT='' 
		self.DB_NAME='' 
		self.DB_SCHEMA=''
		self.loadSettingsFromFile()

	def loadSettingsFromFile(self):
		try:
			f = open('.connection','r')
			connectionstr = f.read()
			f.close()
			connarr = connectionstr.split(' - ')

			self.DB_USERNAME	= connarr[0]
			self.DB_HOST			= connarr[1] 
			self.DB_PORT			= connarr[2]
			self.DB_NAME			= connarr[3]
			self.DB_SCHEMA		= connarr[4]
			if self.DB_SCHEMA.strip() == '':
				self.DB_SCHEMA = 'public'

		except:
			#saved connection details not found. using defaults
			self.DB_USERNAME='postgres' 
			self.DB_HOST='localhost' 
			self.DB_PORT='1532' 
			self.DB_NAME='postgres' 
			self.DB_SCHEMA='public'		

		try:
			f = open('.pwd','r')
			self.DB_USERPWD = f.read()
			f.close()
		except:
			self.DB_USERPWD='no-password-supplied'

	def savepwd(self,pwd):
		f = open('.pwd','w')
		f.write(pwd)
		f.close()

	def dbconnectionstr(self):
		return 'usr=' + self.DB_USERNAME + '; svr=' + self.DB_HOST + '; port=' + self.DB_PORT + '; Database=' + self.DB_NAME + '; Schema=' + self.DB_SCHEMA + '; pwd=' + self.DB_USERPWD

	def saveConnectionDefaults(self,DB_USERNAME='postgres',DB_USERPWD='no-password-supplied',DB_HOST='localhost',DB_PORT='1532',DB_NAME='postgres',DB_SCHEMA='public'):
		f = open('.pwd','w')
		f.write(DB_USERPWD)
		f.close()

		f = open('.connection','w')
		f.write(DB_USERNAME + ' - ' + DB_HOST + ' - ' + DB_PORT + ' - ' + DB_NAME + ' - ' + DB_SCHEMA)
		f.close()

		self.loadSettingsFromFile()

class db:
	def dbversion(self):
		return self.queryone('SELECT VERSION()')

	def __init__(self,DB_USERPWD='no-password-supplied',DB_SCHEMA='no-schema-supplied'):
		self.db_conn_dets = dbconnection_details()
		self.dbconn = None
		self.cur = None

		if DB_USERPWD != 'no-password-supplied':
			self.db_conn_dets.DB_USERPWD = DB_USERPWD			#if you pass in a password it overwrites the stored pwd

		if DB_SCHEMA != 'no-schema-supplied':
			self.db_conn_dets.DB_SCHEMA = DB_SCHEMA			#if you pass in a schema it overwrites the stored schema


	def savepwd(self,pwd):
		self.db_conn_dets.savepwd(pwd)

	def saveConnectionDefaults(self,DB_USERNAME,DB_USERPWD,DB_HOST,DB_PORT,DB_NAME,DB_SCHEMA='public'):
		self.db_conn_dets.saveConnectionDefaults(DB_USERNAME,DB_USERPWD,DB_HOST,DB_PORT,DB_NAME,DB_SCHEMA)

	def useConnectionDetails(self,DB_USERNAME,DB_USERPWD,DB_HOST,DB_PORT,DB_NAME,DB_SCHEMA):

		self.db_conn_dets.DB_USERNAME = DB_USERNAME
		self.db_conn_dets.DB_USERPWD = DB_USERPWD			
		self.db_conn_dets.DB_HOST = DB_HOST				
		self.db_conn_dets.DB_PORT = DB_PORT				
		self.db_conn_dets.DB_NAME = DB_NAME					
		self.db_conn_dets.DB_SCHEMA = DB_SCHEMA		
		if self.db_conn_dets.DB_SCHEMA == '':
			self.db_conn_dets.DB_SCHEMA = 'public'
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
			this_schema = self.db_conn_dets.DB_SCHEMA
			this_table = tblname.split('.')[0]

		qualified_table = this_schema + '.' + this_table

		self.export_query_to_csv('SELECT * FROM ' + qualified_table,csvfile,szdelimiter)

	def load_csv_to_table(self,csvfile,tblname,withtruncate=True,szdelimiter=',',fields='',withextrafields={}):
		this_schema = tblname.split('.')[0]
		try:
			this_table = tblname.split('.')[1]
		except:
			this_schema = self.db_conn_dets.DB_SCHEMA
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
			this_schema = self.db_conn_dets.DB_SCHEMA
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
			this_schema = self.db_conn_dets.DB_SCHEMA
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

		if self.db_conn_dets.DB_USERPWD == 'no-password-supplied':
			print('Password must be passed in or stored in order to connect.\n')
			print('Call \n\t savepwd(pwd) to use defaults or \n') 
			print('\t saveConnectionDefaults(DB_USERNAME,DB_USERPWD,DB_HOST,DB_PORT,DB_NAME,DB_SCHEMA)\n\n')

			sys.exit(0)
		else:
			p_options = "-c search_path=" + self.db_conn_dets.DB_SCHEMA
			try:
				if not self.dbconn:
					self.dbconn = psycopg2.connect(
							host=self.db_conn_dets.DB_HOST,
							database=self.db_conn_dets.DB_NAME,
							user=self.db_conn_dets.DB_USERNAME,
							password=self.db_conn_dets.DB_USERPWD,
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

if __name__ == '__main__':
	print ("db command line") # 
	print('')

	mydb = db()
	mydb.saveConnectionDefaults('postgres','4165605869','localhost','1532','postgres','public')

	print(mydb.db_conn_dets.dbconnectionstr())
	print('')
	mydb.connect()
	print(mydb.dbversion())
	print('')
	qry = """
	SELECT DISTINCT table_catalog as database_name, table_schema as schema 
	FROM INFORMATION_SCHEMA.TABLES
	"""
	print(mydb.export_query_to_str(qry,'\t'))

	mydb.close()	

