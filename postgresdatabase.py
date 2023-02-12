"""
  Dave Skura, 2022
"""
import os
import sys
import psycopg2 
from dbvars import dbinfo

class db:
	def setvars(self,DB_USERNAME,DB_USERPWD,DB_HOST,DB_PORT,DB_NAME,DB_SCHEMA):
		self.ihost = DB_HOST				
		self.iport = DB_PORT				
		self.idb = DB_NAME					
		self.ischema = DB_SCHEMA		
		if self.ischema == '':
			self.ischema = 'public'
		self.iuser = DB_USERNAME
		self.ipwd = DB_USERPWD			

		self.dbconn = None
		self.cur = None
		self.connect()

	def __init__(self):
		self.version=1.0
		
		# ***** edit these DB credentials for the installation to work *****
		self.ihost = dbinfo().DB_HOST				# 'localhost'
		self.iport = dbinfo().DB_PORT				#	"5432"
		self.idb = dbinfo().DB_NAME					# 'nfl'
		self.ischema = dbinfo().DB_SCHEMA		#	'_raw'
		if self.ischema == '':
			self.ischema = 'public'
		self.iuser = dbinfo().DB_USERNAME		#	'dad'
		self.ipwd = dbinfo().DB_USERPWD			#	'dad'
		self.connection_str = dbinfo().dbconnectionstr

		self.dbconn = None
		self.cur = None

	def export_zetl(self):
		
		etl_list = self.query('SELECT DISTINCT etl_name FROM ' + self.ischema + '.z_etl ORDER BY etl_name')
		for etl in etl_list:
			etl_name = etl[0]
			qry = "SELECT etl_name,stepnum,sqlfile,steptablename,estrowcount FROM " + self.ischema + ".z_etl WHERE etl_name = '" + etl_name + "' ORDER BY stepnum"
			csv_filename = 'zetl_scripts\\' + etl_name + '\\z_etl.csv'
			self.export_query_to_csv(qry,csv_filename)

	def load_z_etlcsv_if_forced(self,etl_name='',option=''):
		szdelimiter = ','
		if (etl_name != '' and option == '-f'):
			qualified_table = self.ischema + ".z_etl"
			dsql = "DELETE FROM " +  qualified_table + " WHERE etl_name = '" + etl_name + "'"
			self.execute(dsql)
			self.commit()
			csv_filename = 'zetl_scripts\\' + etl_name + '\\z_etl.csv'
			f = open(csv_filename,'r')
			hdrs = f.read(1000).split('\n')[0].strip().split(szdelimiter)
			f.close()		
			
			isqlhdr = 'INSERT INTO ' + qualified_table + '('

			for i in range(0,len(hdrs)):
				isqlhdr += hdrs[i] + ','
			isqlhdr = isqlhdr[:-1] + ') VALUES '

			skiprow1 = 0
			ilines = ''

			with open(csv_filename) as myfile:
				for line in myfile:
					if skiprow1 == 0:
						skiprow1 = 1
					else:
						row = line.rstrip("\n").split(szdelimiter)

						newline = '('
						for j in range(0,len(row)):
							if row[j].lower() == 'none' or row[j].lower() == 'null':
								newline += "NULL,"
							else:
								newline += "'" + row[j].replace(',','').replace("'",'') + "',"
							
						ilines += newline[:-1] + ')'
						
						qry = isqlhdr + ilines
						ilines = ''
						self.execute(qry)
						self.commit()

	def is_an_int(self,prm):
			try:
				if int(prm) == int(prm):
					return True
				else:
					return False
			except:
					return False

	def add_etl_step(self,p_etl_name,p_etl_step,p_etl_filename):
		isql = "INSERT INTO " + self.ischema + ".z_etl(etl_name,stepnum,sqlfile) VALUES ('" + p_etl_name + "'," + p_etl_step + ",'" + p_etl_filename + "')"
		self.execute(isql)
		self.commit()
		print('Adding ' + p_etl_name + '\\' + p_etl_filename)

	def etl_step_exists(self,etl_name,etl_step):
		sql = "SELECT COUNT(*) FROM " + self.ischema + ".z_etl WHERE etl_name = '" + etl_name + "' and stepnum = " + etl_step
		etlrowcount = self.queryone(sql)
		if etlrowcount == 0:
			return False
		else:
			return True

	def load_folders_to_zetl(self,this_etl_name='all'):
		etl_folder = 'zetl_scripts'
		subdirs = [x[0] for x in os.walk(etl_folder)]
		for i in range(0,len(subdirs)):
			possible_etl_dir = subdirs[i]
			if len(possible_etl_dir.split('\\')) == 2:
				etl_name = possible_etl_dir.split('\\')[1]
				if (this_etl_name == 'all' or etl_name == this_etl_name):
					
					dir_list = os.listdir(etl_folder + '\\' + etl_name)
					for etl_script_file in os.listdir(etl_folder + '\\' + etl_name):
						if etl_script_file.endswith(".sql"):
							if len(etl_script_file.split('.')) == 3:
								etl_step = etl_script_file.split('.')[0]
								file_suffix = etl_script_file.split('.')[1] + '.' + etl_script_file.split('.')[2]
								if self.is_an_int(etl_step):
									if not self.etl_step_exists(etl_name,etl_step):
										self.add_etl_step(etl_name,etl_step,etl_script_file)		

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
						if 'loadfile' in withextrafields: # loadfile, stationid, province
							newline += "'" + withextrafields['loadfile']  + "',"
						if 'stationid' in withextrafields: # loadfile, stationid,province
							newline += "'" + withextrafields['stationid'] + "',"
						if 'province' in withextrafields: # loadfile, stationid, province
							newline += "'" + withextrafields['province'] + "',"

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
			self.dbconn = psycopg2.connect(
					host=self.ihost,
					database=self.idb,
					user=self.iuser,
					password=self.ipwd,
					options=p_options
					#autocommit=True
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



