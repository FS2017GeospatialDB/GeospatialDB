from cassandra.cluster import Cluster
import uuid
import time

def Insert(date, session, id, part, feature):
	tuid = uuid.uuid1()

	ps19 = """
	INSERT INTO NODE_PLEVEL19(id, part_lv19, time, feature)
	VALUES (%s,%s,%s,'%s')
	"""%(id, part, tuid, feature)

	ps19in = """
	INSERT INTO inserted_p19 (date, id, part_lv19, time, feature)
	VALUES ('%s', %s, %s, %s, '%s')
	"""%(date,id, part, tuid, feature)

	#print(ps19in)
	#print(date)
	session.execute(ps19)
	session.execute(ps19in)

cluster = Cluster()
session = cluster.connect('global')

foo = "this is rubbish"
date = time.strftime("%Y-%m-%d")
Insert(date,session,1234567890,9876543210, foo)
