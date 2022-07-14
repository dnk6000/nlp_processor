SELECT 
            git200_crawl.sn_accounts.id_project,
            git200_crawl.sn_accounts.id_www_sources,
            git200_crawl.sn_accounts.account_id
          FROM
            git200_crawl.sn_accounts
          WHERE
            git200_crawl.sn_accounts.id_www_sources = 3 AND 
            git200_crawl.sn_accounts.id_project = 9 AND 
            (git200_crawl.sn_accounts.is_broken IS NULL OR git200_crawl.sn_accounts.is_broken = FALSE) AND
            git200_crawl.sn_accounts.account_closed = FALSE AND
            git200_crawl.sn_accounts.account_type = 'G' 
          ORDER BY
            git200_crawl.sn_accounts.num_subscribers DESC,
            git200_crawl.sn_accounts.id
            
            
DELETE FROM 
  git200_crawl.queue 
WHERE 
  id_project = 9
;

SELECT 
  gid,
  id,
  name,
  full_name,
  date_deep,
  recrawl_days_post,
  recrawl_days_reply,
  requests_delay_sec
FROM 
  git000_cfg.projects 
WHERE id = 10;

SELECT * FROM git000_cfg.get_project_params(10)

--QUEUE with Num Subscribers
SELECT 
  q.id, q.id_project, q.id_www_sources, q.num_order, 
  q.sn_id, a.num_subscribers, q.url, q.is_process,
  q.date_start_process, q.date_end_process, q.date_deferred, q.attempts_counter
FROM 
  git200_crawl.queue AS q
LEFT JOIN git200_crawl.sn_accounts AS a ON (q.sn_id = a.account_id AND q.id_project = a.id_project)
WHERE q.id_project = 9
ORDER BY a.num_subscribers DESC;

SELECT * FROM git200_crawl.queue AS q 
  WHERE q.date_start_process IS NULL
  
UPDATE git200_crawl.queue 
  SET is_process = FALSE
  WHERE date_start_process IS NULL

---------------------------------------------  
SELECT 
  gid,
  id,
  id_www_sources,
  id_project,
  sn_id,
  sn_post_id,
  last_date,
  last_date::timestamp  - '1 year'::interval AS corr_last_date,
  upd_date
FROM 
  git200_crawl.sn_activity 
WHERE sn_id = '108397580';

DELETE FROM 
  git200_crawl.sn_activity 
WHERE 
  sn_id = '108397580';

UPDATE 
  git200_crawl.sn_activity 
SET 
  last_date = last_date::timestamp  - '1 year'::interval
WHERE last_date > '07.01.2021';

SELECT '2010-05-06'::date + interval '1 month 1 day 1 minute' AS result;

SELECT 
  gid,
  id,
  id_project,
  id_www_sources,
  date_in,
  is_process,
  is_broken,
  content_header,
  content_date,
  sn_id,
  sn_post_id,
  sn_post_parent_id
FROM 
  git300_scrap.data_text 
WHERE content_date > '07.01.2021';

UPDATE 
  git300_scrap.data_text  
SET 
  content_date = content_date::timestamp  - '1 year'::interval
WHERE content_date > '07.01.2021';

UPDATE 
  git200_crawl.queue 
SET 
  is_process = FALSE
WHERE 
  id = 836278;

SELECT 
  gid,
  id,
  id_project,
  id_www_sources,
  num_order,
  sn_id,
  url,
  is_process,
  date_start_process,
  date_end_process,
  date_deferred,
  attempts_counter
FROM 
  git200_crawl.queue 
  WHERE sn_id = '108397580';

SELECT * FROM pg_stat_activity
	WHERE application_name not like '%EMS%';

SELECT pg_terminate_backend(pid)
  FROM pg_stat_activity
 WHERE usename = 'postgres' AND application_name not like '%EMS%'

/*
SELECT COUNT(*) FROM pg_stat_activity ;

SELECT * FROM pg_stat_activity WHERE usename = 'm.tyurin' AND application_name not like '%111%';
--SELECT * FROM pg_stat_activity WHERE usename = 'm.tyurin' AND application_name not like '%EMS%';

SELECT pg_backend_pid()

--pg_terminate_backend(pid) --kill

--kill group: https://stackoverflow.com/questions/5108876/kill-a-postgresql-session-connection
*/

--Ã¿——»¬€
select *,55 from unnest(ARRAY[1,2], ARRAY['foo','bar','baz']) as x(a,b)

select * FROM unnest(ARRAY[1,2], 55, ARRAY['rrr', 'bbb']) as x(a,n,b)