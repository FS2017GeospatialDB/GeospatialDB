TABLE features {
	level		int,
	s2ID		bigint,
	time		timestamp,
	id			uuid,
	geoJson		text,	

	PRIMARY KEY((level, s2ID), time, id)
}