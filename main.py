import json
from datetime import datetime
import pandas as pd

# from lib.session_handlers.event_service import EventLogger, Event
from lib.object_factory.node_factory import Node
from lib.dataframe_factory.dfx import dfxc
from lib.query_compiler.cypher import cypher_compiler


# Start session
# global log 
# log = EventLogger()

# kickoff = Event("Operational","Starting process", log)
# kickoff.publish()
# del kickoff
