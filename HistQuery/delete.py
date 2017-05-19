from cassandra.cluster import Cluster
from cassandra.query import named_tuple_factory
import uuid
import time

def Delete(date, session, id, part):
	select = """
	SELECT * FROM NODE_PLEVEL19
	WHERE part_lv19 = %s AND id = %s
	"""%(part,id)
	rows = session.execute(select);

	for row in rows:
		ps19de = """
		INSERT INTO deleted_p19 (date, id, part_lv19, time, feature)
		VALUES ('%s',%s,%s,%s,'%s')
		"""%(date,row.id,row.part_lv19,row.time,row.feature)
		session.execute(ps19de)

	ps19 = """
	DELETE FROM NODE_PLEVEL19
	WHERE part_lv19 = %s AND id = %s
	"""%(part,id)
	session.execute(ps19)


cluster = Cluster()
session = cluster.connect('global')
session.row_factory = named_tuple_factory

date = time.strftime("%Y-%m-%d")
Delete(date,session,1,1)
