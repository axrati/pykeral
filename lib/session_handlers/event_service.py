import json
from datetime import datetime


class EventLogger:
    history = []
    def __init__(self):
        self.process_start_date = datetime.now()
    def publish(self,item):
        self.history.append(item)
    def help(self):
        print("""
              Accessible properites:
                  self.history === An array of events
                  self.process_start_date === Start of process
              
              
              Event Accessible properities:
                  self.initiated_ts === timestamp of event
                  self.type === Categorical event type
                  self.desc === Description of event
                  self.add() === Send additional data
                  self.additional_data === An array of any extra items to investigate on this Event
                  self.publish === Add to the logging service
              """)

class Event:
    # Capture multiple streamed notes on a particular event
    additional_data = []
    def __init__(self, event_type, event_text, logger):
        self.initiated_ts = datetime.now()
        self.type = event_type
        self.desc = event_text
        self.logger = logger
    
    def add(self,event):
        sub_event_ts = datetime.now()
        self.additional_data.append([{"data":event,"time":sub_event_ts}])
        
    def publish(self):
        self.logger.publish(self)
        
    def help(self):
        print("""
              Accessible properities:
                  self.add() === Send additional data (string of info)
                  self.additional_data === An array of any extra items to investigate on this Event
                        schema for each item is { "data":"sample data", "time": datetime }
                  self.publish === Add to the logging service
              """)
        

