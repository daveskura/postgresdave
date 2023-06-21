
# using query_to_xml

	select table_name, table_schema, 
	query_to_xml(format('select count(*) as cnt from %I.%I', table_schema, table_name), false, true, '') as xml_count
	from information_schema.tables
	where table_name='bqdatasets' and table_schema = 'public'; --<< change here for the schema you want


	select table_name, table_schema, 
	     query_to_xml(
		concat('select count(*) as cnt from ', table_schema, '.',table_name)
	     , false, true, '') as xml_count
	from information_schema.tables
	where table_name='bqdatasets' and table_schema = 'public';

# use xpath

	select table_schema, 
	       table_name, 
	       (xpath('/row/cnt/text()', xml_count))[1]::text::int as row_count
	from (
		select table_name, table_schema, 
		     query_to_xml(
			concat('select count(*) as cnt from ', table_schema, '.',table_name)
		     , false, true, '') as xml_count
		from information_schema.tables
		where table_name='bqdatasets' and table_schema = 'public' 
	) t;
