SELECT 
  ent.id,
  ent.id_project,
  ent.id_www_source,
  ent.id_data_text,
  ent.id_sentences,
  ent.id_ent_type,
  ent.created,
  ent.txt_lemm,
  ent.is_process,
  CASE WHEN ent.geocode_source = 1 THEN 'FIAS'
  WHEN ent.geocode_source = 2 THEN 'TripAdv'
  ELSE '' END,
  ROUND(ent.latitude::numeric,6)::varchar||';'||ROUND(ent.longitude::numeric,6)::varchar AS ForMap,
  data_text.content
FROM 
  git430_ner.entity AS ent
LEFT JOIN git300_scrap.data_text AS data_text ON (id_data_text = data_text.id)
WHERE
  ent.id_ent_type = 16
LIMIT 1000;


---------------------------------------------------------
SELECT * FROM git999_log.trace('Start GeoBinding');

--------------------------------------------------------- 
-------- Log
SELECT 
  id,
  gid,
  created,
  id_log_level,
  message,
  id_project,
  node,
  node_table,
  other
FROM 
  git999_log.log 
WHERE message = 'Start GeoBinding';


------------------------------------------------ ARCHIVE 1
WITH ne_per AS (SELECT 
  id_data_text AS per_id_data_text,
  txt_lemm AS per_txt_lemm
FROM 
  git430_ner.entity 
WHERE
  id_ent_type = 15 
  AND (similarity(txt_lemm, 'текслер') > 0.5 OR similarity(txt_lemm, 'текслер алексей') > 0.5)
),
assemble AS (
    SELECT 
      ne_loc.id,
      ne_loc.id_project,
      ne_loc.id_www_source,
      ne_loc.id_data_text,
      ne_loc.id_sentences,
      ne_loc.id_ent_type,
      ne_loc.created,
      ne_loc.txt_lemm,
      ne_loc.longitude,
      ne_loc.latitude,
      ne_loc.is_process,
      ne_loc.geocode_source,
      per_txt_lemm AS per_txt_lemm,
      git300_scrap.data_text.content,
      git700_rate.text.id_rating_type,
      git700_rate.rating_type.name AS rate,
      git010_dict.www_sources.name AS source,
      git300_scrap.data_text.sn_id,
      git300_scrap.data_text.sn_post_id,
      git200_crawl.sn_accounts.account_name,
      CASE
        WHEN ne_loc.id_www_source = 3 THEN git200_crawl.get_source_url_vk(git300_scrap.data_text.sn_id, git300_scrap.data_text.sn_post_id)
        WHEN ne_loc.id_www_source = 4 THEN git200_crawl.get_source_url_tg(git200_crawl.sn_accounts.account_name, git300_scrap.data_text.sn_post_id)
        ELSE ''
      END AS url
    FROM 
      git430_ner.entity AS ne_loc
    INNER JOIN ne_per ON (id_data_text = per_id_data_text)
    LEFT JOIN git300_scrap.data_text ON (git300_scrap.data_text.id = per_id_data_text)
    LEFT JOIN git700_rate.text ON (git700_rate.text.id_data_text = per_id_data_text)
    LEFT JOIN git700_rate.rating_type ON (git700_rate.text.id_rating_type = git700_rate.rating_type.id)
    LEFT JOIN git010_dict.www_sources ON (ne_loc.id_www_source = git010_dict.www_sources.id)
    LEFT JOIN git200_crawl.sn_accounts ON (
        git300_scrap.data_text.id_project = git200_crawl.sn_accounts.id_project AND
        git300_scrap.data_text.id_www_sources = git200_crawl.sn_accounts.id_www_sources AND
        git300_scrap.data_text.sn_id = git200_crawl.sn_accounts.account_id )
    WHERE
      id_ent_type = 16
      AND ne_loc.longitude <> 0 
      AND ne_loc.latitude <> 0 
)
SELECT * FROM assemble



------------------------------------------------ ARCHIVE 2
WITH ne_per AS (SELECT 
  id_data_text AS per_id_data_text,
  txt_lemm AS per_txt_lemm
FROM 
  git430_ner.entity 
WHERE
  id_ent_type = 15 
  --AND (similarity(txt_lemm, 'текслер') > 0.5 OR similarity(txt_lemm, 'текслер алексей') > 0.5)
LIMIT 30000
),
assemble AS (
    SELECT 
      row_number() OVER() AS row_num,
      ne_loc.id,
      ne_loc.id_project,
      ne_loc.id_www_source,
      ne_loc.id_data_text,
      ne_loc.id_sentences,
      ne_loc.id_ent_type,
      git300_scrap.data_text.content_date,
      ne_loc.txt_lemm,
      ne_loc.longitude,
      ne_loc.latitude,
      ne_loc.is_process,
      ne_loc.geocode_source,
      per_txt_lemm AS per_txt_lemm,
      git300_scrap.data_text.content,
      git700_rate.text.id_rating_type,
      git700_rate.rating_type.name AS tonality,
      git010_dict.www_sources.name AS source,
      git300_scrap.data_text.sn_id,
      git300_scrap.data_text.sn_post_id,
      git200_crawl.sn_accounts.account_name,
      CASE
        WHEN ne_loc.id_www_source = 3 THEN git200_crawl.get_source_url_vk(git300_scrap.data_text.sn_id, git300_scrap.data_text.sn_post_id)
        WHEN ne_loc.id_www_source = 4 THEN git200_crawl.get_source_url_tg(git200_crawl.sn_accounts.account_name, git300_scrap.data_text.sn_post_id)
        ELSE ''
      END AS url
    FROM 
      git430_ner.entity AS ne_loc
    INNER JOIN ne_per ON (id_data_text = per_id_data_text)
    LEFT JOIN git300_scrap.data_text ON (git300_scrap.data_text.id = per_id_data_text)
    LEFT JOIN git700_rate.text ON (git700_rate.text.id_data_text = per_id_data_text)
    LEFT JOIN git700_rate.rating_type ON (git700_rate.text.id_rating_type = git700_rate.rating_type.id)
    LEFT JOIN git010_dict.www_sources ON (ne_loc.id_www_source = git010_dict.www_sources.id)
    LEFT JOIN git200_crawl.sn_accounts ON (
        git300_scrap.data_text.id_project = git200_crawl.sn_accounts.id_project AND
        git300_scrap.data_text.id_www_sources = git200_crawl.sn_accounts.id_www_sources AND
        git300_scrap.data_text.sn_id = git200_crawl.sn_accounts.account_id )
    WHERE
      id_ent_type = 16
      AND ne_loc.longitude <> 0 
      AND ne_loc.latitude <> 0 
),
doubles AS (
SELECT 
    assemble.id,
    COUNT(*) AS num_id,
    max(assemble.row_num) AS row_num
FROM assemble
GROUP BY assemble.id
HAVING COUNT(*) > 1
)
SELECT 
  assemble.content_date AS date,
  assemble.tonality,
  assemble.longitude,
  assemble.latitude,
  assemble.source,
  assemble.url,
  assemble.content
FROM assemble
INNER JOIN doubles ON (doubles.row_num = assemble.row_num)
WHERE assemble.tonality <> 'Skip' AND assemble.tonality <> 'Speech'