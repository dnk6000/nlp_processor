----------------------------------------------------------------------- ARRAY
unnest(string_to_array(group_search_str, ',')::varchar[]) AS search_str
WHERE sn_acc.id_project = ANY (ARRAY[9, 11, 14, 15, 16]::INT[])
select *,55 from unnest(ARRAY[1,2], ARRAY['foo','bar','baz']) as x(a,b)
select * FROM unnest(ARRAY[1,2], 55, ARRAY['rrr', 'bbb']) as x(a,n,b)

----------------------------------------------------------------------- DATE TIME
WHERE fin_date > '2022.02.09 10:30:00'
WHERE content_date > '07.01.2021'
SELECT date_trunc('hour', TIMESTAMP '2001-02-16 20:38:40');  
	Результат: 2001-02-16 20:00:00 --can be: 'hour','year','day'...
last_date::timestamp  - '1 year'::interval AS corr_last_date

----------------------------------------------------------------------- NULL
AND coalesce(parameters,'') <> ''

----------------------------------------------------------------------- SESSIONS
SELECT * FROM pg_stat_activity WHERE usename = 'm.tyurin' 
    AND application_name LIKE '%EMS%'
SELECT * FROM pg_stat_activity 
    WHERE application_name NOT LIKE '%EMS%';
SELECT pg_terminate_backend(17347);
SELECT pg_terminate_backend(pid) FROM pg_stat_activity 
   WHERE usename = 'postgres' AND application_name NOT LIKE '%EMS%'

----------------------------------------------------------------------- EXPLAIN
EXPLAIN (analyze, costs off) 
      SELECT git300_scrap.data_text.id -- ...
        FROM  git300_scrap.data_text 
        WHERE git300_scrap.data_text.id_project = 10 -- ...

----------------------------------------------------------------------- RE-SEARCH
WHERE account_name ~* '.*Фурсов.*' and account_name ~* '.*Андрей.*';
WHERE message LIKE 'Job step start%'
    AND other LIKE 'id = ' || '201' || '%'
    
----------------------------------------------------------------------- SIMILARITY
WHERE similarity("T1".content_header, "T2".content_header) > 0.5 

----------------------------------------------------------------------- DIFFERENT
SELECT DISTINCT --...

----------------------------------------------------------------------- PAUSE
CREATE EXTENSION pg_sleep;
select pg_sleep(1)

----------------------------------------------------------------------- DBLINK
SELECT * FROM pg_catalog
CREATE EXTENSION dblink;
SELECT * FROM dblink('dbname=postgres', 'select 1') AS t1(p1 integer)
SELECT * FROM dblink('dbname=postgres options=-csearch_path=',
                     'select proname, prosrc from pg_proc')
  AS t1(proname name, prosrc text) WHERE proname LIKE 'bytea%';
SELECT * FROM dblink('dbname=postgres', 'SELECT jlgjobid, jlgstatus
   FROM pgagent.pga_joblog') AS t1(jlgjobid integer, jlgstatus char )
   
----------------------------------------------------------------------- WINDOW FUNC   
SELECT sn_id, id_www_sources, id_project, row_number() OVER() as num_order

----------------------------------------------------------------------- HANDMADE TABLE
WITH
  departments(department, created_at) AS (
    VALUES ('Dept 1', DATE '2017-01-10'),
           ('Dept 2', DATE '2017-01-11'),
           ('Dept 3', DATE '2017-01-12'),
           ('Dept 4', DATE '2017-04-01'),
           ('Dept 5', DATE '2017-04-02')
  )
  
----------------------------------------------------------------------- LATERAL & GENERATE SERIES
WITH
  departments(department, created_at) AS (
    VALUES ('Dept 1', DATE '2017-01-10'),
           ('Dept 2', DATE '2017-01-11'),
           ('Dept 3', DATE '2017-01-12'),
           ('Dept 4', DATE '2017-04-01'),
           ('Dept 5', DATE '2017-04-02')
  )

SELECT *
FROM departments AS d
CROSS JOIN LATERAL generate_series(
  d.created_at, -- We can dereference a column from department!
  '2017-01-31'::TIMESTAMP,
  INTERVAL '1 day'
) AS days(day)