'''Apply new schema'''

from cassandra.cluster import Cluster


def apply_schema(schema_name):
    '''apply any schema been passed to this function'''
    cluster = Cluster()
    session = cluster.connect('global')

