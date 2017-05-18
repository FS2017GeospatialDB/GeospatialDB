CREATE KEYSPACE COLORADO
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

CREATE TABLE NODE(
    id        uuid,
    time      timeuuid,
    lat       float,
    lon       float,
    movable   boolean,
    version   int,
    feature   blob,
    PRIMARY KEY (movable, lon, lat, id)
);

CREATE TABLE COMPLEX(
    id        uuid,
    time      timeuuid,
    u_lat     float,
    d_lat     float,
    l_lon     float,
    r_lon     float,
    movable   boolean,
    version   int,
    feature   blob,
    PRIMARY KEY (movable, l_lon, r_lon, u_lat, d_lat, id)
);

CREATE TABLE TIME_MACHINE(
    id        uuid,         -- id of the modified feature
    type      ascii,        -- type of ... (NODE or COMPLEX)
    mod_date  date,         -- this is added as a partition, and it is easier to query the data based on range of date
    mod_time  timeuuid,  
    lat       float,        -- for COMPLEX it should have 4 coords, but as a past record we just keep the center of the feature.
    lon       float,        -- It is not perfect, but as a past record, one would only check the point to which he's interested.
                            -- If he wants to check the past record of the feature, chance is high he would focus on the center 
                            -- of the feature, or the feature simply doesn't draw the attention. 
    movable   boolean,
    version   int,          -- each time an edition, version increases by 1
    feature   blob,
    PRIMARY KEY ((date, movable), lon, lat, mod_time)
);