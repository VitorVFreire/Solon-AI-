from typing import List, Dict, Optional
from pathlib import Path
from src import Neo4jConnection
from dotenv import load_dotenv
import json
import os
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Carrega variáveis de ambiente
load_dotenv()

class Companies:
    """Gerencia a criação de nós e relacionamentos de empresas no Neo4j com base em atividades econômicas."""
    
    def __init__(self, conn: Neo4jConnection, folders: Dict[str, List[Path]]):
        """
        Inicializa a classe Companies.

        Args:
            conn (Neo4jConnection): Conexão com o banco Neo4j.
            folders (Dict[str, List[Path]]): Dicionário com listas de caminhos de arquivos JSON.
        """
        self.conn = conn
        self.folders = folders
        self.nodes: List[Dict] = []
        self.activities: List[Dict] = []
        self._load_activities()

    def _load_activities(self) -> None:
        """Carrega atividades econômicas a partir de arquivos JSON."""
        try:
            for file in self.folders.get('activities', []):
                with open(file, 'r', encoding='utf-8') as f:
                    activity = json.load(f)
                    self.activities.append(activity)
                logger.info(f"Carregado arquivo de atividades: {file}")
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Erro ao carregar atividades de {file}: {e}")
            raise

    def find_nodes_by_service(self, service: str, attribute: str) -> List[Dict]:
        """
        Busca nós que possuem o serviço especificado em um atributo.

        Args:
            service (str): Serviço a ser buscado.
            attribute (str): Atributo onde o serviço será procurado.

        Returns:
            List[Dict]: Lista de nós que contêm o serviço.
        """
        service = service.lower()
        return [
            node for node in self.nodes
            if any(service in str(s).lower() for s in node.get(attribute, []))
        ]

    def create_nodes(self) -> None:
        """Cria nós de empresas no Neo4j a partir de arquivos JSON."""
        try:
            for file in self.folders.get('companies', []):
                with open(file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    node = self.conn.create_node('Company', data)
                    data['id'] = node.id
                    self.nodes.append(data)
                    logger.info(f"Nó criado para empresa: {data.get('company_name')}")
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Erro ao criar nós a partir de {file}: {e}")
            raise

    def create_relationships(self) -> None:
        """Cria relacionamentos entre empresas com base em atividades econômicas e correlatas."""
        for activity in self.activities:
            economic_activity = activity.get('economic_activity', '')
            
            # Relacionamentos para mesma atividade
            self._create_relationships_for_activity(
                economic_activity, 'same_activity', {
                    'economic_activity': economic_activity,
                    'sector_classification': activity.get('sector_classification', ''),
                    'economic_context': activity.get('economic_context', ''),
                    'correlation_level': 5,
                    'dependency_level': 1.00
                }
            )

            # Relacionamentos para atividades correlatas
            for correlated in activity.get('correlated_activities', []):
                self._create_relationships_for_activity(
                    correlated.get('activity', ''), 'correlated_activities', {
                        'economic_activity': correlated.get('activity', ''),
                        'correlation_type': correlated.get('correlation_type', ''),
                        'correlation_description': correlated.get('correlation_description', ''),
                        'economic_justification': correlated.get('economic_justification', ''),
                        'dependency_level': correlated.get('dependency_level', 0.0),
                        'correlation_level': correlated.get('correlation_level', 0)
                    }
                )

    def _create_relationships_for_activity(self, activity: str, rel_type: str, properties: Dict) -> None:
        """
        Cria relacionamentos entre nós para uma atividade específica.

        Args:
            activity (str): Atividade econômica a ser buscada.
            rel_type (str): Tipo de relacionamento a ser criado.
            properties (Dict): Propriedades do relacionamento.
        """
        for attribute in ['sector', 'main_products_services', 'industry', 'subsector', 'primary_activity']:
            nodes = self.find_nodes_by_service(activity, attribute)
            if nodes:
                for i, node1 in enumerate(nodes):
                    for node2 in nodes[i+1:]:  # Evita duplicatas e auto-relacionamentos
                        try:
                            self.conn.create_relationship(
                                node1['id'], node2['id'], rel_type, properties
                            )
                            logger.info(f"Relacionamento criado: {node1['company_name']} -> {node2['company_name']} ({rel_type})")
                        except Exception as e:
                            logger.error(f"Erro ao criar relacionamento entre {node1['id']} e {node2['id']}: {e}")
                break  # Sai do loop se encontrou nós