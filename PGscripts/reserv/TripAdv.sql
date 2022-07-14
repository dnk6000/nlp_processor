--Fias + Trip Advisor
WITH prep_fias AS (
  SELECT 
    id,
    aoguid,
    full_addr,
    full_addr_2,
    similarity(ffa.full_addr, 'шизарт') AS sim,
    similarity(ffa.full_addr_2, 'шизарт') AS sim2
  FROM 
    git010_dict.full_fias_addr AS ffa
),
fias AS (
  SELECT *, GREATEST(sim, sim2) AS sim_max FROM prep_fias
  WHERE GREATEST(sim, sim2) > 0.3
  ORDER BY sim_max DESC
  LIMIT 20
),
prep_trip_adv AS (
   SELECT 
    id,
    name,
    name_lemma,
    address,
    similarity(ffa.name, 'шизарт') AS sim_name,
    similarity(ffa.name_lemma, 'шизарт') AS sim_lemma,
    similarity(ffa.address, 'шизарт') AS sim_addr
  FROM 
    git010_dict.trip_advisor AS ffa
),
trip_adv AS (
  SELECT *, GREATEST(sim_name,sim_lemma,sim_addr) AS sim_max FROM prep_trip_adv
  WHERE GREATEST(sim_name,sim_lemma,sim_addr) > 0.3
  ORDER BY sim_max DESC
  LIMIT 20
)
SELECT 'fias' AS source, sim_max, full_addr AS fld_1, full_addr_2 AS fld_2, '' AS fld_3 
	FROM fias

UNION ALL

SELECT 'trip_adv', sim_max, name, name_lemma, address FROM trip_adv

ORDER BY sim_max DESC
;



--Fias
WITH preptab AS (
  SELECT 
    id,
    aoguid,
    full_addr,
    full_addr_2,
    similarity(ffa.full_addr, 'миасс') AS sim,
    similarity(ffa.full_addr_2, 'миасс') AS sim2
  FROM 
    git010_dict.full_fias_addr AS ffa
)
SELECT *, sim+sim2 AS sim_summ FROM preptab
WHERE sim + sim2 > 0.3
ORDER BY sim_summ DESC
LIMIT 50;

--Trip Advisor
WITH preptab AS (
  SELECT 
    id,
    name,
    name_lemma,
    address,
    similarity(ffa.name, 'зимний сад') AS sim_name,
    similarity(ffa.name_lemma, 'зимний сад') AS sim_lemma,
    similarity(ffa.address, 'зимний сад') AS sim_addr
  FROM 
    git010_dict.trip_advisor AS ffa
)
SELECT *, GREATEST(sim_name,sim_lemma,sim_addr) AS sim_max FROM preptab
WHERE GREATEST(sim_name,sim_lemma,sim_addr) > 0.3
ORDER BY sim_max DESC
LIMIT 50;


SELECT 
  id,
  gid,
  name,
  name_lemma,
  name2,
  address,
  category_str,
  longtitude,
  latitude,
  url,
  date_in
FROM 
  git010_dict.trip_advisor ;






SELECT 
  id,
  full_addr,
  replace(full_addr, 'челябинская обл', '')
FROM 
  git010_dict.full_fias_addr 
LIMIT 100
;

UPDATE 
  git010_dict.full_fias_addr 
SET 
  full_addr_2 = replace(t.full_addr, 'челябинская обл', '')
FROM (
  SELECT 
    id,
    full_addr
  FROM 
    git010_dict.full_fias_addr
   
) AS t
WHERE 
  git010_dict.full_fias_addr.id = t.id
;

SELECT 
  id,
  full_addr,
  full_addr_2
FROM 
  git010_dict.full_fias_addr 
LIMIT 100
;