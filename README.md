# postgresdave
My database wrapper on psycopg2

# examples

### command line
py -m postgresdave_package.postgresdave

### sample python program
from postgresdave_package.postgresdave import db 

print('sample program\n')

mydb = db()

mydb.connect()

print(mydb.dbversion())

print(' - - - - - - - - - - - - - - - - - - - - - - - - - - -  \n')

print('table_count = ' + str(mydb.queryone('SELECT COUNT(*) as table_count FROM INFORMATION_SCHEMA.TABLES')))

print(' - - - - - - - - - - - - - - - - - - - - - - - - - - -  \n')

qry = """
SELECT DISTINCT table_catalog as database_name, table_schema as schema 
FROM INFORMATION_SCHEMA.TABLES
"""

print(mydb.export_query_to_str(qry,'\t'))

mydb.close()		

