--���������� ������� git010_dict.full_fias_addr aoguid ������� ��������� �����
INSERT INTO git010_dict.full_fias_addr
( aoguid )
 (
  SELECT DISTINCT
    addr.fias_house.aoguid
  FROM
    addr.fias_addrobj
    INNER JOIN addr.fias_house ON (addr.fias_addrobj.aoguid = addr.fias_house.aoguid)
    LEFT OUTER JOIN git010_dict.full_fias_addr ON (addr.fias_house.aoguid = git010_dict.full_fias_addr.aoguid)
  WHERE
    addr.fias_addrobj.regioncode = 74 AND
    git010_dict.full_fias_addr.id IS NULL
);

--fill in 'full_fias_addr' by regional entities
INSERT INTO git010_dict.full_fias_addr
( aoguid )
 (
    WITH regions AS (
      SELECT 
        id_actstat,
        aoguid,
        formalname,
        shortname
      FROM 
        addr.fias_addrobj 
      WHERE 
        id_actstat = 1 
        AND regioncode = 74
        AND shortname = '�-�'
    )
    SELECT 
      fa.aoguid
/*      ,
      fa.id_actstat,
      fa.formalname,
      fa.shortname,
      regions.formalname,
      regions.shortname
*/    FROM 
      addr.fias_addrobj AS fa
    INNER JOIN regions ON (fa.parentguid = regions.aoguid)
    WHERE 
      fa.id_actstat = 1 
      AND fa.regioncode = 74
)  

--���������� git010_dict.full_fias_addr ������� ��������
UPDATE
  git010_dict.full_fias_addr
SET
  full_addr = full_name_query.full_addr,
  regioncode = 74
FROM (
    WITH null_full_addr AS (
      SELECT
        id,
        aoguid
      FROM
        git010_dict.full_fias_addr
      WHERE full_addr IS NULL
      )
    SELECT id, aoguid, git010_dict.fias_get_full_addr(aoguid) AS full_addr, 74 AS regioncode
        FROM null_full_addr
	) AS full_name_query
WHERE
  git010_dict.full_fias_addr.id = full_name_query.id
;

------------------------------------------------------------------------
--������

SELECT DISTINCT
  addr.fias_house.aoguid,
  addr.fias_addrobj.regioncode,
  count(addr.fias_addrobj.regioncode) AS NumRecords
FROM
  addr.fias_addrobj
  INNER JOIN addr.fias_house ON (addr.fias_addrobj.aoguid = addr.fias_house.aoguid)
  LEFT OUTER JOIN git010_dict.full_fias_addr ON (addr.fias_house.aoguid = git010_dict.full_fias_addr.aoguid)
WHERE
  (addr.fias_addrobj.regioncode = 74 OR
  addr.fias_addrobj.regioncode = 74) AND
  git010_dict.full_fias_addr.id IS NULL
GROUP BY
  addr.fias_house.aoguid,
  addr.fias_addrobj.regioncode
ORDER BY NumRecords DESC
LIMIT 20

SELECT
  addr.fias_house.aoguid,
  addr.fias_addrobj.regioncode
FROM
  addr.fias_addrobj
  INNER JOIN addr.fias_house ON (addr.fias_addrobj.aoguid = addr.fias_house.aoguid)
  LEFT OUTER JOIN git010_dict.full_fias_addr ON (addr.fias_house.aoguid = git010_dict.full_fias_addr.aoguid)
WHERE
  (addr.fias_addrobj.regioncode = 74 OR
  addr.fias_addrobj.regioncode = 74) AND
  git010_dict.full_fias_addr.id IS NULL
LIMIT 2000


SELECT
  addr.fias_house.aoguid,
  addr.fias_addrobj.regioncode
FROM
  addr.fias_addrobj
  INNER JOIN addr.fias_house ON (addr.fias_addrobj.aoguid = addr.fias_house.aoguid)
  LEFT OUTER JOIN git010_dict.full_fias_addr ON (addr.fias_house.aoguid = git010_dict.full_fias_addr.aoguid)
WHERE
  (addr.fias_addrobj.formalname = '���������') AND
  git010_dict.full_fias_addr.id IS NULL
LIMIT 2000


WITH RECURSIVE child_to_parents AS (
  SELECT addr.fias_addrobj.* FROM addr.fias_addrobj
      WHERE aoguid = '97df5a98-f662-44c6-8a52-2fa3c458ae2c'
  UNION ALL
  SELECT addr.fias_addrobj.* FROM addr.fias_addrobj, child_to_parents
      WHERE addr.fias_addrobj.aoguid = child_to_parents.parentguid
        --AND addr.fias_addrobj.id_curentst = 1
        AND addr.fias_addrobj.id_actstat = 1
)
SELECT * FROM child_to_parents ORDER BY aolevel;