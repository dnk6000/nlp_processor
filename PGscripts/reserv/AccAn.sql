--2 query

--Query 1: Analizing number of subscribers
WITH acc AS (
  SELECT 
    id,
    id_project,
    account_type,
    account_id,
    account_name,
    account_screen_name,
    account_closed,
    num_subscribers,
    is_broken,
    broken_status_code,
    created
  FROM 
    git200_crawl.sn_accounts 
  WHERE
    id_project = 9
    AND not is_broken
    AND not account_closed
),
summ0 AS (
  select 
     CASE
          WHEN num_subscribers >= 10000 THEN 1
          ELSE 0 
     END AS Lev1,
     CASE
          WHEN num_subscribers < 10000 AND num_subscribers >= 5000 THEN 1
          ELSE 0 
     END AS Lev2,
     CASE
          WHEN num_subscribers < 5000 AND num_subscribers >= 1000 THEN 1
          ELSE 0 
     END AS Lev3,
     CASE
          WHEN num_subscribers < 1000 AND num_subscribers >= 300 THEN 1
          ELSE 0 
     END AS Lev4,
     CASE
          WHEN num_subscribers < 300 AND num_subscribers >= 50 THEN 1
          ELSE 0 
     END AS Lev5,
     CASE
          WHEN num_subscribers < 50 THEN 1
          ELSE 0 
     END AS Lev6
  FROM acc),
summ AS (
	SELECT
     sum(Lev1) AS Lev1,
     sum(Lev2) AS Lev2,
     sum(Lev3) AS Lev3,
     sum(Lev4) AS Lev4,
     sum(Lev5) AS Lev5,
     sum(Lev6) AS Lev6
    FROM summ0)
SELECT * FROM summ

--Query 2: Analizing groups
WITH params (
      include_broken,
      include_exist_search_str,
      include_unexist_search_str,
      include_exist_unsearch_str,
      include_unexist_unsearch_str
      ) AS (values(
      False,     --include_broken
      True,  --include_exist_search_str
      not True,  --include_unexist_search_str
      not True,  --include_exist_unsearch_str
      not True   --include_unexist_unsearch_str
      )),
tt_search_str AS (
  SELECT
    1::Integer AS exist,
    unnest(string_to_array(group_search_str,',')) AS key_word
  FROM
    git000_cfg.projects
  WHERE id = 9

  UNION ALL
  	SELECT 1, unnest(ARRAY['чел€б','чебарку','magnitogorsk','zlatoust','ozersk','troitsk','snezhinsk','74','сатка','satka'])

),
tt_unsearch_str AS (
  SELECT
    1::Integer AS exist,
    unnest(string_to_array(group_unsearch_str,',')) AS key_word
  FROM
    git000_cfg.projects
  WHERE id = 9
),
tt_acc AS (SELECT
  id,
  id_www_sources,
  id_project,
  account_type,
  account_id,
  lower(account_name) AS account_name,
  lower(account_screen_name) AS account_screen_name,
  account_closed,
  num_subscribers,
  is_broken,
  broken_status_code,
  git200_crawl.get_source_url_vk(account_id, '') AS url,
  created,
  updated
FROM
  git200_crawl.sn_accounts, params
WHERE id_project = 9
	  AND coalesce(created,'2022.01.01 00:00:00') < '2022.06.23 00:00:00'
      AND not is_broken is Null
      AND (include_broken AND coalesce(is_broken, False) OR not is_broken)
),
tt_analize_0 AS (
	SELECT
        tt_acc.id,
    	tt_acc.account_name,
    	tt_acc.account_screen_name,
    	tt_acc.account_closed,
    	tt_acc.num_subscribers,
    	tt_acc.is_broken,
    	tt_acc.broken_status_code,
        tt_acc.url,
    	CASE WHEN
          (SELECT max(tt_search_str.exist)
     		FROM tt_search_str
     		WHERE
            	position(tt_search_str.key_word in tt_acc.account_name)>0
                OR position(tt_search_str.key_word in tt_acc.account_screen_name)>0
  		  ) is Null THEN False
          ELSE True
        END AS exist_search_str,
    	CASE WHEN
          (SELECT max(tt_unsearch_str.exist)
     		FROM tt_unsearch_str
     		WHERE
            	position(tt_unsearch_str.key_word in tt_acc.account_name)>0
                OR position(tt_unsearch_str.key_word in tt_acc.account_screen_name)>0
  		  ) is Null THEN False
          ELSE True
        END AS exist_unsearch_str
    FROM tt_acc),
tt_analize_1 AS (
SELECT t.*,
  params.include_exist_search_str AND t.exist_search_str AS inc_1_1,
  params.include_unexist_search_str AND not t.exist_search_str AS inc_1_2,
  params.include_exist_unsearch_str AND t.exist_unsearch_str AS inc_2_1,
  params.include_unexist_unsearch_str AND not t.exist_unsearch_str AS inc_2_2
FROM tt_analize_0 AS t, params
),
tt_analize_2 AS (
SELECT * FROM tt_analize_1 WHERE (inc_1_1 OR inc_1_2 OR inc_2_1 OR inc_2_2)
ORDER BY id
LIMIT 100000)
SELECT * FROM tt_analize_2 ORDER BY num_subscribers DESC

/*UPDATE
  git200_crawl.sn_accounts d
SET
  is_broken = True,
  broken_status_code = '001'
FROM tt_analize_2 t
WHERE
  d.id = t.id
;*/