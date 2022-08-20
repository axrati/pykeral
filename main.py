import json
from datetime import datetime
from xmlrpc.client import Boolean
import pandas as pd

from lib.session_handlers.event_service import EventLogger, Event
from lib.object_factory.node_factory import Node

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



#3) Publish to Neo4j or API