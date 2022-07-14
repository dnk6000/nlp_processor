--DATA TEXT Analize: sn_id / num messages / min_date
WITH TT_text_count as (SELECT
  sn_id,
  max(id_project) as id_project,
  count(id) as num_rec
  --min(CASE WHEN content_date = '2001.01.01 00:00:00' THEN now() ELSE content_date END) as content_date
FROM
  git300_scrap.data_text
WHERE
  id_project = 9
  AND date_in > '2022.07.07 00:00:00'
GROUP BY
  git300_scrap.data_text.sn_id)
SELECT  coalesce(TT_text_count.sn_id, q.sn_id) as sn_id,
		coalesce(sn_acc.num_subscribers,0) as num_subscribers,
        sn_acc.account_name,
        TT_text_count.num_rec,
        --TT_text_count.content_date,
        CASE WHEN TT_text_count.sn_id IS NULL THEN 0 ELSE 1 END as num_group
	FROM TT_text_count
    LEFT JOIN git200_crawl.sn_accounts AS sn_acc
    	ON (sn_acc.id_project = TT_text_count.id_project
            AND sn_acc.account_id = TT_text_count.sn_id)
    RIGHT JOIN git200_crawl.queue AS q
    	ON (q.id_project = 9
            AND q.sn_id = TT_text_count.sn_id)
	WHERE q.id_project = 9
UNION ALL
	SELECT 'TOTAL:', 99999999999, NULL, sum(num_rec), count(num_rec)
    	FROM TT_text_count
ORDER BY num_subscribers DESC;

--QUEUE with Num Subscribers
SELECT
  q.id, q.id_project, q.id_process, q.id_www_sources, q.num_order,
  q.sn_id, a.num_subscribers, 
  q.is_process,
  q.date_start_process, q.date_end_process,
  coalesce(q.date_end_process, now()) - q.date_start_process AS Duration,
  q.date_deferred, q.attempts_counter, q.comment,
  a.account_name, a.suitable_degree, git200_crawl.get_source_url_vk(a.account_id) AS url
FROM
  git200_crawl.queue AS q
LEFT JOIN git200_crawl.sn_accounts AS a ON (q.sn_id = a.account_id AND q.id_project = a.id_project)
WHERE 
	q.id_project = 9 
    AND (q.id_process is Null OR q.id_process = 0)
--AND not q.is_process --and not q.date_deferred is NULL
ORDER BY q.num_order, a.num_subscribers DESC
LIMIT 1000;
--ORDER BY a.num_subscribers DESC;




UPDATE git200_crawl.queue SET is_process = TRUE WHERE id_project = 9 AND sn_id = '98296371'

--NUM SUBSCRIBERS
SELECT
  id, id_www_sources, id_project, account_type, account_id, account_name, account_screen_name,
  account_closed, is_broken, num_subscribers
FROM
  git200_crawl.sn_accounts
WHERE id_project = 9
AND num_subscribers > 10000;

--ACCOUNTS
SELECT
  gid,
  id,
  id_www_sources,
  id_project,
  account_type,
  account_id,
  account_name,
  account_screen_name,
  account_closed,
  num_subscribers,
  is_broken,
  broken_status_code,
  account_extra_1,
  git200_crawl.get_source_url_vk(account_id, ''),
  created,
  updated
FROM
  git200_crawl.sn_accounts
WHERE id_project = 9
	  AND created >= '2022.06.23 00:00:00'
      AND is_broken is Null
      --AND id % 3 = 0
      --AND is_broken is not Null
      --AND coalesce(is_broken, False)
     -- AND not coalesce(is_broken, False)
      --AND account_id = '211563662'
    --  AND num_subscribers = 0
ORDER BY num_subscribers DESC;

-------------------------------------------------
SELECT
  count(*), max(date_in)
FROM
  git200_crawl.data_html
WHERE
  id_project = 9

SELECT
  id,
  id_project,
  id_www_sources,
  id_data_html,
  date_in,
  is_process,
  is_broken,
  content_date,
  sn_id,
  sn_post_id,
  sn_post_parent_id,
  content
FROM
  git300_scrap.data_text
WHERE
  content_date > '2021.09.20 00:00:00'
  AND content_date <> '0001.01.01 00:00:00'
  AND id_project = 9
LIMIT 1000;

SELECT * FROM git200_crawl.queue_generate(3, 9);

CALL git_test.tm_test2(7)

SELECT * FROM git000_cfg.need_stop_func('crawl_wall', 7);

SELECT * FROM git000_cfg.set_config_param('stop_func_crawl_wall', '');