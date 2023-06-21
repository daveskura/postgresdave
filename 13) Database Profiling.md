# Analyzing Postgres

## List schemas
```
SELECT distinct table_schema
FROM information_schema.tables
WHERE table_schema not in ('pg_catalog','information_schema');

SELECT DISTINCT schemaname
FROM pg_tables
WHERE schemaname not in ('pg_catalog','information_schema');

```

## List table/view counts by schema
```
SELECT table_schema,table_type,count(*)
FROM information_schema.tables
WHERE table_schema not in ('pg_catalog','information_schema')
GROUP BY table_schema,table_type
ORDER BY table_schema,table_type;

SELECT schemaname,count(*)
FROM pg_tables
WHERE schemaname not in ('pg_catalog','information_schema')
GROUP BY schemaname
ORDER BY schemaname;
```


## List count(*) for each table in Schema Public
```
select table_schema, 
       table_name, 
       (xpath('/row/cnt/text()', xml_count))[1]::text::int as row_count
from (
  select table_name, table_schema, 
         query_to_xml(format('select count(*) as cnt from %I.%I', table_schema, table_name), false, true, '') as xml_count
  from information_schema.tables
  where table_schema = 'public' --<< change here for the schema you want
) t
```