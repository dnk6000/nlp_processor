--convert to xls   from Excel to csv     Attention to the delimiter fo decimal types


--How to import CSV file data into a PostgreSQL table?
--Источник <https://stackoverflow.com/questions/2987433/how-to-import-csv-file-data-into-a-postgresql-table> 

--Importing CSV Files to PostgreSQL Databases
--Источник <https://web.archive.org/web/20101030205652/http://ensode.net/postgresql_csv_import.html> 

--on the server--

COPY zip_codes FROM '/path/to/csv/ZIP_CODES.txt' WITH (FORMAT csv);

COPY zip_codes FROM '/path/to/csv/ZIP_CODES.txt' DELIMITER ',' CSV HEADER;


COPY git_test.building_centroids2 FROM '/opt2/pgpython/modules/Temp/building_centroids2xls.csv' WITH (FORMAT csv DELIMITER ';');

COPY git_test.building_centroids2 FROM '/opt2/pgpython/modules/Temp/building_centroids2xls.csv' DELIMITER ';' CSV HEADER;
