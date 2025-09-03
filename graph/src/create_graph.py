from typing import List, Dict, Optional
from pathlib import Path
from src import Neo4jConnection
from dotenv import load_dotenv
import json
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

class Companies:
    def __init__(self, conn: Neo4jConnection, folders: Dict[str, List[Path]]):
        self.conn = conn
        self.folders = folders
        self.nodes: List[Dict] = []
        self.activities: List[Dict] = []
        self.relationship: List = []
        self._load_activities()

    def _load_activities(self) -> None:
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
        service = service.lower()
        return [
            node for node in self.nodes
            if any(service in str(s).lower() for s in node.get(attribute, []))
        ]

    def create_nodes(self) -> None:
        try:
            for file in self.folders.get('companies', []):
                with open(file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                    node = self.conn.create_node('Company', data)
                    data['id'] = node
                    self.nodes.append(data)
                    logger.info(f"Nó criado para empresa: {data.get('company_name')}")
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Erro ao criar nós a partir de {file}: {e}")
            raise

    def create_relationships(self) -> None:
        for activity in self.activities:
            economic_activity = activity.get('economic_activity', '')

            self._create_relationships_for_same_activity(
                economic_activity, {
                    'economic_activity': economic_activity,
                    'sector_classification': activity.get('sector_classification', ''),
                    'economic_context': activity.get('economic_context', ''),
                    'correlation_level': 5,
                    'dependency_level': 1.00
                }
            )
            
            for correlated in activity.get('correlated_activities', []):
                self._create_relationships_for_correlated_activities(
                    economic_activity, correlated.get('activity', ''), {
                        'economic_activity': correlated.get('activity', ''),
                        'correlation_type': correlated.get('correlation_type', ''),
                        'correlation_description': correlated.get('correlation_description', ''),
                        'economic_justification': correlated.get('economic_justification', ''),
                        'dependency_level': correlated.get('dependency_level', 0.0),
                        'correlation_level': correlated.get('correlation_level', 0)
                    }
                )
            logger.info(f"Relações baseado na atividade: {economic_activity}")

    def _create_relationships_for_same_activity(self, activity: str, properties: Dict) -> None:
        relationship_id = ''
        relationship_id_temp = ''
        attribute = 'main_products_services'
        nodes = self.find_nodes_by_service(activity, attribute)
        if nodes:
            for i, node1 in enumerate(nodes):
                for node2 in nodes[i+1:]:
                    relationship_id_temp = relationship_id.join([node1['id'],node2['id']])
                    if not relationship_id_temp in self.relationship:
                        try:
                            self.conn.create_relationship(
                                node1['id'], node2['id'], 'same_activity', properties
                            )
                            self.relationship.append(relationship_id_temp)
                            logger.info(f"Relacionamento criado: {node1['company_name']} -> {node2['company_name']} ({'same_activity'})")
                        except Exception as e:
                            logger.error(f"Erro ao criar relacionamento entre {node1['id']} e {node2['id']}: {e}")
                break
    
    def _create_relationships_for_correlated_activities(self, main_activity:str, activity: str, properties: Dict) -> None:
        relationship_id = ''
        relationship_id_temp = ''
        attribute = 'main_products_services'
        nodes = self.find_nodes_by_service(main_activity, attribute)
        if nodes:
            nodes2 = self.find_nodes_by_service(activity, attribute)
            for i, node1 in enumerate(nodes):
                for node2 in nodes2:
                    relationship_id_temp = relationship_id.join([node1['id'],node2['id']])
                    if not relationship_id_temp in self.relationship:
                        try:
                            self.conn.create_relationship(
                                node1['id'], node2['id'], 'correlated_activities', properties
                            )
                            self.relationship.append(relationship_id_temp)
                            logger.info(f"Relacionamento criado: {node1['company_name']} -> {node2['company_name']} ({'correlated_activities'})")
                        except Exception as e:
                            logger.error(f"Erro ao criar relacionamento entre {node1['id']} e {node2['id']}: {e}")
                break