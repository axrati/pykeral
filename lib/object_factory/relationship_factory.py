import uuid

class Relationship:
    
    def __init__(self, schema={}, from_to_nodes={}, label="Unknown" ):
        if type(schema)==dict:
            self.id = str(uuid.uuid4())
            self.label = label
            self.data = {**schema}
            self.from_to_nodes = from_to_nodes
        else:
            raise Exception("Schema of Relationships need to be type dict")
            

    
    def help(self):
        print("""

Accessible properities:

  self.id === Auto-generated id
  self.data === Dictionary of relationship data. At a minumium as keys of "name", "rel_group_name", "derived" and "schema". The other keys are supplied in row_attributes, but isnt required
  self.label === Should really be set
  self.from_to_nodes === From/To Node IDs

  self.gen_many() === For duplicating of labeled node
      Example:
          foo = Relationship(schema={ }, from_to_nodes={ } label="HAS_INTEREST_IN")
          relationships = foo.gen_many(
                  [
                     {"from":"id#123", "to":"id#898", "schema":{"key":"value1"},
                     {"from":"id#123", "to":"id#898", "schema":{"key":"value2" },
                  ]
              )

              """)


