import uuid
from lib.utils.tools import new_vs_update

class Node:
    
    def __init__(self, contents={}, label="Unknown" ):
        if type(contents)==dict:
            self.id = str(uuid.uuid4())
            self.label = label
            self.data = {**contents}
        else:
            raise Exception("Contents of Node need to be type dict")
            
    def gen_many(self,array_of_self):
        factory = []
        for clone in array_of_self:
            factory.append(
                Node(clone,label=self.label)
                )
        return factory

    def edit(self,node_data):
        if type(node_data)!=dict:
            raise Exception("Editing a Node attribute must be a key/value dict { }")
        
        new_keys, update_keys = new_vs_update(list(node_data.keys()),list(self.data.keys()))
        for key in new_keys:
            self.data[key] = node_data[key]
        for key in update_keys:
            self.data.update({key:node_data[key]})
        
    def delete(self,key):
        if type(key)!=list:
            raise Exception("Deleting Node attributes must be a list of keys [ ]")
        del self.data[key]




    def help(self):
        print("""
Accessible properities:

  self.id === Auto-generated id
  self.data === Dictionary of node data
  self.label === Should really be set
  self.gen_many() === For duplicating of labeled node
      Example:
          foo = Node({ },"Person")
          persons = foo.gen_many(
                  [
                     {"name":"Joe", "age":1}
                     {"name":"Alex", "age":2}
                  ]
              )

              """)



