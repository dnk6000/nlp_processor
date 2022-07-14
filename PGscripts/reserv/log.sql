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
    (id_project = 9 OR id_project = 10)
    --id_project = 10
    AND created > '2022.03.13 16:00:00'
    --AND message <> 'NUM_SUBSCRIBERS Not found'
    AND message <> 'Date not recognize'
   -- AND message <> 'FINISH Success'
   --AND message = 'FINISH Success'
   --AND other LIKE '%meduzalive%'
ORDER BY created;
    
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
WHERE
	id_log_level = 4 AND id_project = 7;
    
    
SELECT 
  log.id, log.created, log.id_log_level, lvl.name, log.message,
  log.id_project, log.node, log.node_table, log.other
FROM 
  git999_log.log as log
LEFT JOIN git999_log.log_level as lvl on (lvl.id = log.id_log_level )  
WHERE
    -- TRACE = 0 DEBUG = 1 INFO  = 2 WARN  = 3 ERROR = 4 FATAL = 5
	TRUE
	--AND id_log_level = 5  
    --(id_log_level = 2 OR id_log_level = 5) AND
    AND (lvl.name = 'FATAL'
    		OR lvl.name = 'ERROR')
    --AND (id_project = 10)
    AND created > '2022.06.23 00:00:00'
    --AND message <> 'NUM_SUBSCRIBERS Not found'
    AND message <> 'Date not recognize'
    AND message <> 'Sentence with mix-letters words'
    AND message <> 'Ner''s mismatch Lemma''s Error'
    AND message <> 'Sentence is too long'
    AND message <> 'Too many entities in sentence'
    --AND message <> 'sentence_set_is_process() got an unexpected keyword argument ''autocommit'''
    --AND message = 'FINISH Success'
    --AND message <> 'FINISH Success'
    --AND message <> 'Data processor'
    --AND message <> 'Get portion for NerProcessor'
    --AND message <> 'Get portion for SentimentProcessor'
    --AND message <> 'input sequence after bert tokenization shouldn''t exceed 512 tokens.'
    --AND message = 'Job step start'
ORDER BY created
LIMIT 1000;

-- JOB Logging
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
	--id_log_level <> 5 AND 
    --(id_log_level = 2 OR id_log_level = 5) AND
    (id_project = 0)
    AND created > '2022.04.04 00:00:00'
    AND not message LIKE '%Geo%'
    AND other LIKE 'id = 30%'
    --AND message LIKE 'Job%'
    --AND ((not other LIKE 'id = 201%') AND (not other LIKE 'id = 101%'))
ORDER BY created;