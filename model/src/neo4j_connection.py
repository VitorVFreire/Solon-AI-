from neo4j import GraphDatabase, exceptions
import logging

logger = logging.getLogger(__name__)

class Neo4jConnection:
    def __init__(self, uri, user, pwd):
        self._uri = uri
        self._user = user
        self._pwd = pwd
        self.driver = None
        try:
            self.driver = GraphDatabase.driver(self._uri, auth=(self._user, self._pwd))
            self.driver.verify_connectivity()
            logger.info("Conexão Neo4j estabelecida e verificada.")
        except exceptions.ServiceUnavailable as e:
            logger.error(f"Não foi possível conectar ao Neo4j em {self._uri}: {e}")
            raise
        except exceptions.AuthError as e:
            logger.error(f"Erro de autenticação com Neo4j: {e}")
            raise
        except Exception as e:
            logger.error(f"Um erro inesperado ocorreu ao conectar ao Neo4j: {e}")
            raise


    def close(self):
        if self.driver:
            self.driver.close()
            logger.info("Conexão Neo4j fechada.")

    def execute_query(self, query, parameters=None, database=None):
        assert self.driver is not None, "Driver não inicializado!"
        session = None
        response = None
        try:
            session = self.driver.session(database=database) if database else self.driver.session()
            response_obj = session.run(query, parameters)
            response = [dict(record) for record in response_obj] # Converte para lista de dicionários
        except Exception as e:
            logger.error(f"Erro na query: {query} \nParâmetros: {parameters} \nErro: {e}")
            raise
        finally:
            if session is not None:
                session.close()
        return response