from cassandra.cluster import Cluster

def connect_to(list):
    for node in list:
        try:
            CLUSTER = Cluster([node])
        except:
            pass
	return CLUSTER
