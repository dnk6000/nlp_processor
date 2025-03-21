--DATA TEXT Analize: sn_id / num messages / min_date
WITH TT_text_count as (SELECT
  sn_id,
  max(id_project) as id_project,
  count(id) as num_rec
  --min(CASE WHEN content_date = '2001.01.01 00:00:00' THEN now() ELSE content_date END) as content_date
FROM
  git300_scrap.data_text
WHERE
  id_project = 10
  AND date_in > '2021.08.02 00:00:00'
GROUP BY
  git300_scrap.data_text.sn_id)
SELECT  coalesce(TT_text_count.sn_id, q.sn_id) as sn_id,
		coalesce(sn_acc.num_subscribers,0) as num_subscribers,
        sn_acc.account_name, sn_acc.account_id,
        TT_text_count.num_rec,
        --TT_text_count.content_date,
        CASE WHEN TT_text_count.sn_id IS NULL THEN 0 ELSE 1 END as num_group
	FROM TT_text_count
    LEFT JOIN git200_crawl.sn_accounts AS sn_acc
    	ON (sn_acc.id_project = TT_text_count.id_project
            AND sn_acc.account_id = TT_text_count.sn_id)
    RIGHT JOIN git200_crawl.queue AS q
    	ON (q.id_project = 10
            AND q.sn_id = TT_text_count.sn_id)
	WHERE q.id_project = 10
UNION ALL
	SELECT 'TOTAL:', 99999999999, NULL, '', sum(num_rec), count(num_rec)
    	FROM TT_text_count
ORDER BY num_subscribers DESC;


--QUEUE with Num Subscribers
SELECT
  q.id, q.id_project, q.id_www_sources, q.num_order,
  q.sn_id, a.num_subscribers, a.account_name, q.is_process,
  q.date_start_process, q.date_end_process, q.date_deferred, q.attempts_counter
FROM
  git200_crawl.queue AS q
LEFT JOIN git200_crawl.sn_accounts AS a ON (q.sn_id = a.account_id AND q.id_project = a.id_project)
WHERE q.id_project = 10 --AND not q.is_process
ORDER BY q.num_order, a.num_subscribers DESC;

SELECT
  log.id,
  log.gid,
  log.created,
  log.id_log_level,
  lvl.name,
  log.message,
  log.id_project,
  log.node,
  log.node_table,
  log.other
FROM
  git999_log.log as log
LEFT JOIN git999_log.log_level as lvl on (lvl.id = log.id_log_level )
WHERE
	--id_log_level = 5 AND
    id_project = 10
    AND
    created > '2021.09.02 00:00:00'
    --AND message <> 'NUM_SUBSCRIBERS Not found'
    --AND message <> 'Date not recognize'
    --AND message <> 'FINISH Success'
ORDER BY created;