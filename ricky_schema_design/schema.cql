TABLE features {
	level		int,
	s2ID		bigint,
	time		timestamp,
	id			uuid,
	geoJson		text,	

	PRIMARY KEY((level, s2ID), time, id)
}

# For this schema, lets use:
# 4, 8, 12, 16
#
#
#