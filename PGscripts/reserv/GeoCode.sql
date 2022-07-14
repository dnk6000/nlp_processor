UPDATE
  git010_dict.full_fias_addr AS dest_table
SET
  latitude = geo_codes_complete.latitude,
  longitude = geo_codes_complete.longitude
FROM (
    WITH ffa AS (SELECT 
      git010_dict.full_fias_addr.id,
      git010_dict.full_fias_addr.aoguid,
      git010_dict.full_fias_addr.full_addr,
      addr.fias_addrobj.formalname,
      addr.fias_addrobj.code,
      left(addr.fias_addrobj.code,11) AS code_11,
      left(addr.fias_addrobj.code,8) AS code_8,
      addr.fias_addrobj.shortname
    FROM
      addr.fias_addrobj
      RIGHT JOIN git010_dict.full_fias_addr ON (addr.fias_addrobj.aoguid = git010_dict.full_fias_addr.aoguid)
    WHERE addr.fias_addrobj.id_actstat = 1
     --  AND	git010_dict.full_fias_addr.id = 32802   --DEBUG
      AND git010_dict.full_fias_addr.latitude IS NULL  --DEBUG
      LIMIT 1000 --DEBUG
    ),
    avg_geo_coord_raw AS (  --join on shortly part of kladr code
      SELECT 
          ffa.id,
          ffa.aoguid,
          ffa.full_addr,
          geo_codes."KLADR_Code" AS kladr_code,
          left(geo_codes."KLADR_Code",11) AS kladr_code_11,
          left(geo_codes."KLADR_Code",8) AS kladr_code_8,
          geo_codes."KLADR_Code",
          geo_codes."POINT_Y" AS latitude,
          geo_codes."POINT_X" AS longitude  
      FROM ffa
      LEFT JOIN git_test.building_centroids2 AS geo_codes ON (ffa.code_8 = left(geo_codes."KLADR_Code",8))
    ),
    avg_geo_coord AS ( --join on longly part of kladr code 
      SELECT 
          ffa.id,
          ffa.aoguid,
          ffa.full_addr,
          geo_codes.kladr_code,
          AVG(geo_codes.latitude) AS latitude,
          AVG(geo_codes.longitude) AS longitude  
      FROM ffa
      LEFT JOIN avg_geo_coord_raw AS geo_codes ON (ffa.code = geo_codes.kladr_code)
      GROUP BY ffa.id,ffa.aoguid,ffa.full_addr,geo_codes.kladr_code
    ),
    avg_geo_coord_11 AS ( --join on first 11 symbols of kladr code //——+–––+√√√+œœœ+””””+ƒƒƒƒ - ””””+ƒƒƒƒ
      SELECT 
          ffa.id,
          ffa.aoguid,
          ffa.full_addr,
          geo_codes.kladr_code_11,
          AVG(geo_codes.latitude) AS latitude,
          AVG(geo_codes.longitude) AS longitude  
      FROM ffa
      LEFT JOIN avg_geo_coord_raw AS geo_codes ON (ffa.code_11 = geo_codes.kladr_code_11)
      GROUP BY ffa.id,ffa.aoguid,ffa.full_addr,geo_codes.kladr_code_11
    ),
    avg_geo_coord_8 AS ( --join on first 8 symbols of kladr code //——+–––+√√√+œœœ+””””+ƒƒƒƒ - œœœ+””””+ƒƒƒƒ
      SELECT 
          ffa.id,
          ffa.aoguid,
          ffa.full_addr,
          geo_codes.kladr_code_8,
          AVG(geo_codes.latitude) AS latitude,
          AVG(geo_codes.longitude) AS longitude  
      FROM ffa
      LEFT JOIN avg_geo_coord_raw AS geo_codes ON (ffa.code_8 = geo_codes.kladr_code_8)
      GROUP BY ffa.id,ffa.aoguid,ffa.full_addr,geo_codes.kladr_code_8
    )
   SELECT 
          ffa.id,
          ffa.aoguid,
          ffa.full_addr,
          avg_geo_coord.latitude AS latitude_full,
          avg_geo_coord.longitude AS longitude_full,
          avg_geo_coord_11.latitude AS latitude_11,
          avg_geo_coord_11.longitude AS longitude_11,
          avg_geo_coord_8.latitude AS latitude_8,
          avg_geo_coord_8.longitude AS longitude_8,
          COALESCE(avg_geo_coord.latitude,avg_geo_coord_11.latitude,avg_geo_coord_8.latitude) AS latitude,
          COALESCE(avg_geo_coord.longitude,avg_geo_coord_11.longitude,avg_geo_coord_8.longitude) AS longitude
    FROM ffa
      LEFT JOIN avg_geo_coord    ON (ffa.aoguid = avg_geo_coord.aoguid)
      LEFT JOIN avg_geo_coord_11 ON (ffa.aoguid = avg_geo_coord_11.aoguid)
      LEFT JOIN avg_geo_coord_8  ON (ffa.aoguid = avg_geo_coord_8.aoguid)
    WHERE 
    	NOT avg_geo_coord.latitude IS NULL
        OR NOT avg_geo_coord_11.latitude IS NULL
        OR NOT avg_geo_coord_8.latitude IS NULL
) AS geo_codes_complete
WHERE
  dest_table.id = geo_codes_complete.id



--œÓ‚ÂÍ‡ Á‡ÔÓÎÌÂÌËˇ „ÂÓÍÓ‰Ó‚
SELECT 
  id,
  aoguid,
  full_addr,
  latitude,
  longitude
FROM 
  git010_dict.full_fias_addr 
WHERE 
  latitude IS NULL
LIMIT 100;
 

SELECT 
  count(*)
FROM 
  git010_dict.full_fias_addr 
WHERE 
  latitude IS NULL;

 

--Select all geo_codes from s.lahvich-table
WITH ffa AS (SELECT 
  git010_dict.full_fias_addr.id,
  git010_dict.full_fias_addr.aoguid,
  git010_dict.full_fias_addr.full_addr,
  addr.fias_addrobj.formalname,
  addr.fias_addrobj.code,
  addr.fias_addrobj.shortname
FROM
  addr.fias_addrobj
  RIGHT JOIN git010_dict.full_fias_addr ON (addr.fias_addrobj.aoguid = git010_dict.full_fias_addr.aoguid)
WHERE addr.fias_addrobj.id_actstat = 1
LIMIT 1)
SELECT ffa.id,ffa.aoguid,ffa.full_addr,geo_codes."KLADR_Code",geo_codes."POINT_X",geo_codes."POINT_Y"  FROM ffa
LEFT JOIN git_test.building_centroids2 AS geo_codes ON (ffa.code = geo_codes."KLADR_Code")