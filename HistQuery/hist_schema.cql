DROP TABLE IF EXISTS inserted_p19;
CREATE TABLE inserted_p19(
	date date,
	id decimal,
	part_lv19 decimal,
	time timeuuid,
	feature text,
	PRIMARY KEY((date), part_lv19, id, time)
);

DROP TABLE IF EXISTS deleted_p19;
CREATE TABLE deleted_p19(
	date date,
	id decimal,
	part_lv19 decimal,
	time timeuuid,
	feature text,
	PRIMARY KEY((date), part_lv19, id, time)
);

