from cassandra.cluster import Cluster
from cassandra.query import named_tuple_factory
import uuid
import time

def HistQuery(date, session):
	query1 = """
	SELECT * FROM inserted_p19 WHERE date = '%s';
	"""%(date)
	rows1 = session.execute(query1)
	
	for row in rows1:
		delete = """
		DELETE FROM node_plevel19
		WHERE part_lv19 = %s AND id = %s
		"""%(row.part_lv19,row.id)
		#print(delete)
		session.execute(delete)

	query2 = """
	SELECT * FROM deleted_p19 WHERE date = '%s';
	"""%(date)
	rows2 = session.execute(query2)
	
	for row in rows2:
		insert = """
		INSERT INTO node_plevel19 (part_lv19,id,time,feature)
		VALUES (%s,%s,%s,'%s')
		"""%(row.part_lv19,row.id,row.time,row.feature)
		#print(insert)
		session.execute(insert)

cluster = Cluster()
session = cluster.connect('global')
session.row_factory = named_tuple_factory

date = time.strftime("%Y-%m-%d")
HistQuery(date,session)
