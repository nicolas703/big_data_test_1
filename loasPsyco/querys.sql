-- Creacion TABLA CLEAN
CREATE TABLE `powerful-star-421901.database.emerald` (
  ID STRING NOT NULL,
  Age STRING NOT NULL,
  Gender STRING NOT NULL,
  Occupation STRING NOT NULL,
  line_of_work STRING NOT NULL,
  time_bp NUMERIC NOT NULL,
  time_dp NUMERIC NOT NULL,
  travel_time NUMERIC NOT NULL,
  easeof_online NUMERIC NOT NULL,
  home_env INTEGER NOT NULL,
  prod_inc NUMERIC NOT NULL,
  sleep_bal NUMERIC NOT NULL,
  new_skill NUMERIC NOT NULL,
  fam_connect NUMERIC NOT NULL,
  relaxed NUMERIC NOT NULL,
  self_time NUMERIC NOT NULL,
  prefer STRING NOT NULL,
  certaindays_hw STRING NOT NULL,
)
PARTITION BY RANGE_BUCKET(home_env, GENERATE_ARRAY(1, 6, 1));


SELECT * FROM `powerful-star-421901.database.iron` where home_env is not null and loaded = false limit 10;
SELECT * FROM `powerful-star-421901.database.iron` where home_env is not null and line_of_work = "test";

-- PASO A CLEAN
-- Crear una tabla temporal para almacenar los registros válidos
CREATE TEMP TABLE valid_records AS
SELECT 
  ID,
  IFNULL(Age, "Unknown") AS Age,
  IFNULL(Gender, "Unknown") AS Gender,
  IFNULL(Occupation, "Unknown") AS Occupation,
  IFNULL(line_of_work, "N/A") AS line_of_work,
  IFNULL(time_bp, 0) AS time_bp,
  IFNULL(time_dp, 0) AS time_dp,
  IFNULL(travel_time, 0) AS travel_time,
  IFNULL(easeof_online, 0) AS easeof_online,
  IFNULL(home_env, 0) AS home_env,
  IFNULL(prod_inc, 0) AS prod_inc,
  IFNULL(sleep_bal, 0) AS sleep_bal,
  IFNULL(new_skill, 0) AS new_skill,
  IFNULL(fam_connect, 0) AS fam_connect,
  IFNULL(relaxed, 0) AS relaxed,
  IFNULL(self_time, 0) AS self_time,
  IFNULL(prefer, "Unknown") AS prefer,
  IFNULL(certaindays_hw, "Unknown") AS certaindays_hw,
FROM 
  `powerful-star-421901.database.iron`
WHERE 
  loaded = false
  AND ID IS NOT NULL
  AND Age IS NOT NULL
  AND Gender IS NOT NULL
  AND Occupation IS NOT NULL
  AND time_bp IS NOT NULL
  AND time_dp IS NOT NULL
  AND travel_time IS NOT NULL
  AND easeof_online IS NOT NULL
  AND home_env BETWEEN 1 AND 5
  AND prod_inc IS NOT NULL
  AND sleep_bal IS NOT NULL
  AND new_skill IS NOT NULL
  AND fam_connect IS NOT NULL
  AND relaxed IS NOT NULL
  AND self_time IS NOT NULL
  AND prefer IS NOT NULL
  AND certaindays_hw IS NOT NULL;

-- Insertar registros válidos en la tabla emerald
INSERT INTO `powerful-star-421901.database.emerald` (
  ID,
  Age,
  Gender,
  Occupation,
  line_of_work,
  time_bp,
  time_dp,
  travel_time,
  easeof_online,
  home_env,
  prod_inc,
  sleep_bal,
  new_skill,
  fam_connect,
  relaxed,
  self_time,
  prefer,
  certaindays_hw
)
SELECT 
  ID,
  Age,
  Gender,
  Occupation,
  line_of_work,
  time_bp,
  time_dp,
  travel_time,
  easeof_online,
  home_env,
  prod_inc,
  sleep_bal,
  new_skill,
  fam_connect,
  relaxed,
  self_time,
  prefer,
  certaindays_hw
FROM 
  valid_records
WHERE 
  home_env is not null;

-- Actualizar los registros en la tabla iron que se insertaron en emerald
UPDATE 
  `powerful-star-421901.database.iron` AS iron
SET 
  loaded = true
WHERE 
  ID IN (SELECT ID FROM valid_records)
  AND home_env is not null;


-- EN CASO DE EMERGENCIA BORRAR TODOS LOS REGISTROS DE EMERALD
delete from `powerful-star-421901.database.emerald` where home_env is not null;
-- TAMBIEN CAMBIAR A FALSE LOS LOADED EN RAW
UPDATE 
  `powerful-star-421901.database.iron` AS iron
SET 
  loaded = false
WHERE 
  loaded = true
  AND home_env is not null;

