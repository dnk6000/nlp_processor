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



   