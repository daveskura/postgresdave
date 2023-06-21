# using mysqldump

##backup selected database

	mysqldump -u [database_username] –p [database_password] [database_name] [tablename] > [database_backup_file.sql]

	mysqldump -u database_username -p database_name > database_backup_file.sql

	"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqldump" -u dave -p -t  -T. ossn ossn_users --fields-terminated-by=','

	mysqldump database_name -u  database_username -p > database_name-$(date +%Y%m%d).sql

	mysqldump database_name -u  database_username -p | gzip > database_name.sql.gz
	
##backup all databases 
	
	mysqldump -u database_username -p --all-databases > all_databases_backup_file.sql

	for database in $(mysql -e 'show databases' -s --skip-column-names); do
		mysqldump $database > "$database.sql";
	done


# using mysql 

## extract a table/query to a file

	"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql" -u dave ossn -h localhost -e "SELECT * FROM ossn_users" > ossn_users.tsv

## extract a table/query to a csv file

	"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql" -u dave ossn -h localhost -e "SELECT * INTO OUTFILE 'F:\\git\\mysql_basics\\ossn_users.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' FROM ossn_users;"

# using SQL

	SELECT * INTO OUTFILE 'F:\\git\\mysql_basics\\ossn_users.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' FROM ossn_users;
