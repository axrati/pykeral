import json
from datetime import datetime
from xmlrpc.client import Boolean
import pandas as pd

from lib.session_handlers.event_service import EventLogger, Event
from lib.object_factory.node_factory import Node
from lib.dataframe_factory.dfx import dfxc

# Start session
global log 
log = EventLogger()

kickoff = Event("Operational","Starting process", log)
kickoff.publish()
del kickoff

#1) Connect to data
df = pd.DataFrame([
    {"id":1234, "name":"alex", "age":142, "gender":"male", "has_kids":True},
    {"id":23453, "name":"joe", "age":122, "gender":"male", "has_kids":False},
    {"id":234523, "name":"ham", "age":12, "gender":"female", "has_kids":True},
    {"id":1234523434, "name":"waw", "age":14, "gender":"female", "has_kids":False},
    {"id":12345234, "name":"evv", "age":16, "gender":"male", "has_kids":True},
    {"id":1453453234, "name":"tee", "age":50, "gender":"male", "has_kids":False},

], columns=['id','name','age', 'gender','has_kids'])

#2) Create Nodes / Rels
dfx = dfxc(df)

config = {
    "nodes":[],
    "relationships":[]
}


NODE_EXAMPLE =  {
    "node_group_name": "data_group_one",
    "label":"Person",
    "row_level_node_keys":['name','age','gender'],
    "one_to_many":[],
    "derived":[]
    # "one_to_many":[
    #                    { 
    #                         "attribute_name":"employment",
    #                         "column_name":"industry", 
    #                         "sub_columns":[
    #                             { "column_name":"occupation_role_name" }
    #                         ]
    #                     }
    #     ],
    # "derived":[
    #     {"attribute_name":"number_of_players", "operation":"COUNTD", "columns":['user_id']}
    # ]
}

config['nodes'].append(NODE_EXAMPLE)


nodes, relationships = dfx.fish(config)

print(dfx.nodes)

dfx.help()



#3) Publish to Neo4j or API