from neo4j import GraphDatabase

class Neo4jConnection:
    def __init__(self, uri, user, pwd):
        self.driver = GraphDatabase.driver(uri, auth=(user, pwd))

    def close(self):
        self.driver.close()

    # Função para criar um nó
    def create_node(self, label, properties):
        with self.driver.session() as session:
            query = f"CREATE (n:{label} $props) RETURN n"
            result = session.run(query, props=properties)
            return result.single()[0]

    # Função para criar uma relação (aresta)
    def create_relationship(self, node1_id, node2_id, rel_type, properties=None):
        with self.driver.session() as session:
            query = (
                f"MATCH (a), (b) "
                f"WHERE id(a) = $node1_id AND id(b) = $node2_id "
                f"CREATE (a)-[r:{rel_type} $props]->(b) "
                f"RETURN r"
            )
            result = session.run(query, node1_id=node1_id, node2_id=node2_id, props=properties or {})
            return result.single()[0]