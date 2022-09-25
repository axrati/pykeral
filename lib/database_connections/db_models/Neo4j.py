from neo4j import GraphDatabase

class Neo4j:
    def __init__(self, host, port, database, username, password ) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.driver = None
        self.uri = "bolt://{}:{}".format(host,port)
        try:
            self.__driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
        except Exception as e:
            print("Failed to create the driver:", e)
        
    def close(self):
        if self.__driver is not None:
            self.__driver.close()
        
    def query(self, query, qi_ind=True):
        db = self.database
        assert self.__driver is not None, "Driver not initialized!"
        # qi_ind is query | interaction indicator. Indicates whether we shoudl expect results back or not
        session = None
        response = None
        try: 
            session = self.__driver.session(database=db) if db is not None else self.__driver.session() 
            if qi_ind:
                response = list(session.run(query))
                return response
            else: 
                session.run(query)
                return True
        except Exception as e:
            print("Query failed:", e)
            return False

        finally: 
            if session is not None:
                session.close()