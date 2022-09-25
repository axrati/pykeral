from lib.utils.tools import all_exist_in
from lib.database_connections.db_models import Neo4j

def connect(system, config):

    config_required_fields = ['host','port','database','username','password']
    config_check = all_exist_in(config_required_fields, config.keys())
    if config_check==False:
        raise Exception("Your configuration for the database did not contain all the required fields. Required Fields:\n{}".format(str(config_required_fields)))

    if system.lower()=="neo4j":
        return Neo4j(
            host = config['host'],
            port = config['port'],
            database = config['database'],
            username = config['username'],
            password = config['password']
        )