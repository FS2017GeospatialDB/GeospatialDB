//////////////////////////////////////////////////////////////////
//
//                      SCHEMA DESIGN V2_huan
//   INSTEAD OF USING 2D COORDINATE (WHICH IS VERY HARD TO CLUSTERING
//   AND EFFICIENTLY QUERYING), THE NEW APPROACH USING S2 GEO LIBRARY
//   AND ANOTHER MAJOR CHANGE IS COMPLETELY ABANDON THE IDEA OF RELATION
//   FOCUS ON OPTIMIZE READ EFFICIENTLY. 
//
//////////////////////////////////////////////////////////////////

DROP KEYSPACE IF EXISTS GLOBAL;
CREATE KEYSPACE GLOBAL WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE GLOBAL;

-- USER-DEFINED UNIT: hm2: square-hundred-meter; xm2: square-ten-meter; 1m2: square-one-meter (square-meter)

-- level:	1 	31878934.9231 km2
-- level:	2 	7969733.73079 km2
-- level:	3 	1992433.43270 km2
-- level:	4 	498108.358174 km2
-- level:	5 	124527.089544 km2
-- level:	6 	31131.7723859 km2
-- level:	7 	7782.94309647 km2
-- level:	8 	1945.73577412 km2
-- level:	9 	486.433943529 km2
-- level:	10 	121.608485882 km2
-- level:	11 	30.4021214706 km2       PICK
-- level:	12 	7.60053036765 km2
-- level:	13 	1.90013259191 km2
-- level:	14 	47.5033147978 hm2
-- level:	15 	11.8758286994 hm2       PICK
-- level:	16 	2.96895717486 hm2
-- level:	17 	74.2239293716 xm2
-- level:	18 	18.5559823429 xm2
-- level:	19 	4.63899558572 xm2       PICK
-- level:	20 	1.15974889643 xm2
-- level:	21 	28.9937224108 1m2
-- level:	22 	7.24843060269 1m2
-- level:	23 	1.81210765067 1m2       PICK - BASE UNIT
-- level:	24 	45.3026912668 dm2
-- level:	25 	11.3256728167 dm2
-- level:	26 	2.83141820418 dm2
-- level:	27 	70.7854551044 cm2
-- level:	28 	17.6963637761 cm2
-- level:	29 	4.42409094403 cm2
-- level:	30 	1.10602273601 cm2

-- THIS TABLE COVERS RANGE IN 465 m2
-- ON AVG (1 FEATURE PER M2) 465 FEATURES STORED
DROP TABLE IF EXISTS NODE_PLEVEL19;
CREATE TABLE NODE_PLEVEL19(
    id          bigint,
    part_lv19   bigint,
    time        timeuuid,
    feature     text,
    PRIMARY KEY (part_lv19, id, time)
);


-- THIS TABLE COVERS RANGE IN 0.1187 km2 => 118700 m2 
-- TOTAL NODES STORED ON AVG = 118.7K FEATURES, A LOT
-- SO SPLIT IT USING ONE LOWER LEVEL: L15/L16 = 4 PARTS
-- EACH PARTION COVERS ~30K FEATURES
DROP TABLE IF EXISTS NODE_PLEVEL15;
CREATE TABLE NODE_PLEVEL15( 
    id          bigint,
    part_lv15   bigint,
    part_lv16   bigint,
    time        timeuuid,
    feature     text,
    PRIMARY KEY (part_lv15, id, time)
);


-- THE SAME PROCESS WITH FURTHER REDUCED LEVEL
-- L11/L12 = 4, ON AVG 7600K FEATURES PER PART
-- TOO MANY FEATURES, FURTHER REDUCE
-- L11/L13 = 16, ON AVG 1900K, STILL A LOT, BUT
-- AFFORDABLE. IF FURTHER REDUCE WILL REDUCE SCALABILITY
DROP TABLE IF EXISTS NODE_PLEVEL11;
CREATE TABLE NODE_PLEVEL11( 
    id          bigint,
    part_lv11   bigint,
    part_lv13   bigint,
    time        timeuuid,
    feature     text,
    PRIMARY KEY (part_lv11, id, time)
);

