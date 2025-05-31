import os
import json
import datetime
import logging
from typing import List, Dict, Any, Optional, Set 
from src.ai_client import AIClient # Garanta que este é o AIClient refatorado
from src.neo4j_connection import Neo4jConnection
from utils import clean_filename # Importa de utils.py

logger = logging.getLogger(__name__)

class NewsProcessor:
    def __init__(self,
                 llm_client: AIClient,
                 neo4j_conn: Optional[Neo4jConnection],
                 prompt_paths: Dict[str, str],
                 output_dir: str = "output/analysis"):
        self.llm_client = llm_client
        self.neo4j_conn = neo4j_conn
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

        self.prompts = {}
        for key, path in prompt_paths.items():
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    self.prompts[key] = f.read()
            except FileNotFoundError:
                logger.error(f"Arquivo de prompt não encontrado: {path}")
                raise
            except Exception as e:
                logger.error(f"Erro ao ler arquivo de prompt {path}: {e}")
                raise
        
        self.known_companies: List[Dict[str, Any]] = self._load_known_entities("company")
        self.known_sectors: List[Dict[str, Any]] = self._load_known_entities("sector")

        logger.info(f"NewsProcessor inicializado. {len(self.known_companies)} empresas e {len(self.known_sectors)} nomes de setores carregados.")

    def _load_known_entities(self, entity_type: str) -> List[Dict[str, Any]]:
        if not self.neo4j_conn:
            logger.warning(f"Conexão Neo4j não disponível. Não foi possível carregar {entity_type}(s).")
            return []
        
        entities: List[Dict[str, Any]] = []
        if entity_type == "company":
            query = "MATCH (c:Company) WHERE c.company_name IS NOT NULL RETURN elementId(c) as id, c.company_name as name, c.sector as sector_property"
            try:
                results = self.neo4j_conn.execute_query(query)
                entities = [
                    {
                        "id": record["id"],
                        "name": record["name"],
                        "sector": record.get("sector_property") 
                    } for record in results if record and record.get("id") and record.get("name")
                ]
                logger.info(f"Carregadas {len(entities)} {entity_type}(s) do Neo4j.")
            except Exception as e:
                logger.error(f"Erro ao carregar {entity_type}(s) do Neo4j: {e}", exc_info=True)
        
        elif entity_type == "sector":
            query = "MATCH (c:Company) WHERE c.sector IS NOT NULL RETURN DISTINCT c.sector as name"
            try:
                results = self.neo4j_conn.execute_query(query)
                entities = [
                    {"name": record["name"]} 
                    for record in results if record and record.get("name")
                ]
                logger.info(f"Carregados {len(entities)} nomes de {entity_type}(s) distintos das propriedades das empresas no Neo4j.")
            except Exception as e:
                logger.error(f"Erro ao carregar nomes de {entity_type}(s) do Neo4j: {e}", exc_info=True)
        
        return entities

    def _identify_entities_in_news(self, news_content: str) -> Dict[str, List[str]]:
        company_names_str = ", ".join(sorted([c['name'] for c in self.known_companies if c.get('name')]))
        sector_names_str = ", ".join(sorted([s['name'] for s in self.known_sectors if s.get('name')]))

        human_prompt = self.prompts['human_entity_identification'].format(
            news=news_content,
            known_companies_list_str=company_names_str,
            known_sectors_list_str=sector_names_str
        )
        system_prompt = self.prompts['system_entity_identification']
        response_str = "Error" 
        try:
            response_str = self.llm_client.invoke(system_prompt, human_prompt)
            identified_data = json.loads(response_str)
            
            valid_companies = [
                comp_name for comp_name in identified_data.get("identified_companies", [])
                if any(known_comp['name'].lower() == comp_name.lower() for known_comp in self.known_companies if known_comp.get('name'))
            ]
            valid_sectors = [
                sec_name for sec_name in identified_data.get("identified_sectors", [])
                if any(known_sec['name'].lower() == sec_name.lower() for known_sec in self.known_sectors if known_sec.get('name')) 
            ]
            
            logger.info(f"Entidades identificadas pela LLM: Empresas: {valid_companies}, Setores: {valid_sectors}")
            return {"companies": valid_companies, "sectors": valid_sectors}
        except json.JSONDecodeError as e:
            logger.error(f"Erro ao decodificar JSON da identificação de entidades: {e}. Resposta: {response_str[:200]}")
            return {"companies": [], "sectors": []}
        except Exception as e:
            logger.error(f"Erro na chamada LLM para identificação de entidades: {e}", exc_info=True)
            return {"companies": [], "sectors": []}

    def _get_impact_analysis(self, news_content: str, profile: str, identified_entities: Dict[str, List[str]]) -> Optional[Dict[str, Any]]:
        identified_companies_str = ", ".join(identified_entities['companies']) if identified_entities['companies'] else "Nenhuma específica"
        identified_sectors_str = ", ".join(identified_entities['sectors']) if identified_entities['sectors'] else "Nenhum específico"

        entity_context = f"Foco principal em empresas: [{identified_companies_str}] e setores: [{identified_sectors_str}]."
        if not identified_entities['companies'] and not identified_entities['sectors']:
            entity_context = "A notícia parece ter um foco econômico geral, sem destacar empresas ou setores específicos das listas conhecidas."
        elif not identified_entities['companies']: # Apenas setores identificados
             entity_context = f"Foco principal nos setores: [{identified_sectors_str}]."
        elif not identified_entities['sectors']: # Apenas empresas identificadas
             entity_context = f"Foco principal nas empresas: [{identified_companies_str}]."
        # Se ambos, a primeira mensagem já cobre.

        human_prompt = self.prompts['human_impact_analysis'].format(
            perfil=profile,
            news=news_content,
            identified_companies_str=identified_companies_str,
            identified_sectors_str=identified_sectors_str,
            entity_context=entity_context
        )
        system_prompt = self.prompts['system_impact_analysis']
        response_str = "Error"

        try:
            response_str = self.llm_client.invoke(system_prompt, human_prompt)
            analysis_data = json.loads(response_str)
            logger.info(f"Análise de impacto para perfil '{profile}' recebida.")
            return analysis_data
        except json.JSONDecodeError as e:
            logger.error(f"Erro ao decodificar JSON da análise de impacto ({profile}): {e}. Resposta: {response_str[:200]}")
            return None
        except Exception as e:
            logger.error(f"Erro na chamada LLM para análise de impacto ({profile}): {e}", exc_info=True)
            return None

    def process_news_item(self, news_item_data: Dict[str, str], processed_news_hashes: Set[int]) -> Optional[Dict[str, Any]]:
        news_content = f"{news_item_data.get('title', '')}\n\n{news_item_data.get('article', '')}"
        if not news_item_data.get('article'):
            logger.warning("Item de notícia sem conteúdo de artigo. Pulando.")
            return None

        news_hash = hash(news_content)
        if news_hash in processed_news_hashes:
            logger.info(f"Notícia duplicada (hash: {news_hash}, Título: {news_item_data.get('title', 'N/A')[:30]}...) já processada. Pulando.")
            return None
        
        logger.info(f"Processando notícia: {news_item_data.get('title', 'Sem Título')[:50]}...")

        identified_entities = self._identify_entities_in_news(news_content)
        
        analysis_results_by_profile = {}
        profiles = ["Conservador", "Moderado", "Agressivo"]

        for profile in profiles:
            logger.debug(f"Gerando análise para perfil: {profile}")
            # Chamada ao método _get_impact_analysis
            profile_analysis = self._get_impact_analysis(news_content, profile, identified_entities)
            
            if profile_analysis: 
                if isinstance(profile_analysis, dict) and \
                   "personal_score" in profile_analysis and \
                   "sector_score" in profile_analysis and \
                   isinstance(profile_analysis.get("justification"), dict):
                    analysis_results_by_profile[profile] = profile_analysis
                else:
                    logger.warning(f"Análise para perfil {profile} com estrutura inesperada: {str(profile_analysis)[:100]}")
                    analysis_results_by_profile[profile] = {"error": f"Estrutura de análise inválida para perfil {profile}"}
            else: 
                analysis_results_by_profile[profile] = {"error": f"Falha ao gerar análise para perfil {profile}"}
        
        final_result = {
            "news_title": news_item_data.get('title', 'N/A'),
            "news_url": news_item_data.get('url', 'N/A'),
            "news_hash": news_hash,
            "news_snippet": news_content[:250] + "...",
            "timestamp": datetime.datetime.now().isoformat(),
            "identified_entities": identified_entities,
            "analysis_by_profile": analysis_results_by_profile,
            "full_news_content": news_content
        }
        
        clean_title = clean_filename(news_item_data.get('title', f'unknown_news_{news_hash}'))
        # Garante que o nome do arquivo não seja excessivamente longo e remove o perfil, já que o JSON é multi-perfil
        output_filename_base = clean_filename(news_item_data.get('title', f'unknown_news_{news_hash}'))
        output_filename = f"{output_filename_base[:60]}_multi_analysis.json" # Nome de arquivo mais curto
        output_path = os.path.join(self.output_dir, output_filename)

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(final_result, f, ensure_ascii=False, indent=2)
            logger.info(f"Resultado salvo em: {output_path}")
        except Exception as e:
            logger.error(f"Erro ao salvar resultado em arquivo {output_path}: {e}")

        if self.neo4j_conn:
            self._save_analysis_to_neo4j(final_result)
        
        processed_news_hashes.add(news_hash) 
        return final_result

    def _save_analysis_to_neo4j(self, analysis_data: Dict[str, Any]):
        if not self.neo4j_conn:
            return

        news_hash = analysis_data["news_hash"]
        
        try:
            news_node_params = {
                "news_hash": news_hash,
                "title": analysis_data["news_title"],
                "url": analysis_data["news_url"],
                "snippet": analysis_data["news_snippet"],
                "full_content": analysis_data["full_news_content"]
            }
            merge_news_query = """
            MERGE (n:News {news_hash: $news_hash})
            ON CREATE SET n.title = $title, n.url = $url, n.snippet = $snippet, n.full_content = $full_content, n.first_analyzed_at = datetime()
            ON MATCH SET n.last_analyzed_at = datetime(), n.url = $url, n.title = $title, n.snippet = $snippet
            RETURN elementId(n) as news_node_id
            """
            query_result = self.neo4j_conn.execute_query(merge_news_query, news_node_params)
            if not query_result or not query_result[0] or "news_node_id" not in query_result[0]:
                logger.error(f"Falha ao obter news_node_id para notícia hash {news_hash}")
                return
            news_node_id = query_result[0]["news_node_id"]
            logger.info(f"Nó de Notícia (ID: {news_node_id}, Hash: {news_hash}) MERGED no Neo4j.")

            for profile, profile_analysis in analysis_data["analysis_by_profile"].items():
                if "error" in profile_analysis or not isinstance(profile_analysis, dict):
                    logger.warning(f"Análise para perfil {profile} (notícia hash {news_hash}) contém erro ou é inválida, não será salva no Neo4j. Data: {str(profile_analysis)[:100]}")
                    continue

                analysis_props = {
                    "profile": profile,
                    "personal_score": profile_analysis.get("personal_score"),
                    "sector_score": profile_analysis.get("sector_score"),
                    "justification_personal": profile_analysis.get("justification", {}).get("personal", ""),
                    "justification_sector": profile_analysis.get("justification", {}).get("sector", ""),
                    "analyzed_at": analysis_data["timestamp"],
                    "identified_companies_str": ", ".join(analysis_data["identified_entities"]["companies"]),
                    "identified_sectors_str": ", ".join(analysis_data["identified_entities"]["sectors"])
                }
                
                create_analysis_node_query = "CREATE (a:Analysis $props) RETURN elementId(a) as analysis_node_id"
                analysis_query_result = self.neo4j_conn.execute_query(create_analysis_node_query, {"props": analysis_props})
                if not analysis_query_result or not analysis_query_result[0] or "analysis_node_id" not in analysis_query_result[0]:
                     logger.error(f"Falha ao criar nó de Análise para perfil {profile}, notícia {news_hash}")
                     continue
                analysis_node_id = analysis_query_result[0]["analysis_node_id"]

                link_news_analysis_query = """
                MATCH (n:News), (a:Analysis)
                WHERE elementId(n) = $news_id AND elementId(a) = $analysis_id
                MERGE (n)-[:HAS_ANALYSIS]->(a)
                """
                self.neo4j_conn.execute_query(link_news_analysis_query, {"news_id": news_node_id, "analysis_id": analysis_node_id})
                logger.info(f"Análise (ID: {analysis_node_id}, Perfil: {profile}) criada e ligada à Notícia (ID: {news_node_id}).")

                for company_name in analysis_data["identified_entities"]["companies"]:
                    comp = next((c for c in self.known_companies if c.get('name') == company_name), None)
                    if comp and comp.get('id') is not None: 
                        link_analysis_entity_query = """
                        MATCH (a:Analysis), (e:Company)
                        WHERE elementId(a) = $analysis_id AND elementId(e) = $entity_id
                        MERGE (a)-[:ANALYZES_ENTITY {entity_type: 'Company'}]->(e)
                        """
                        self.neo4j_conn.execute_query(link_analysis_entity_query, {"analysis_id": analysis_node_id, "entity_id": comp['id']})
                        logger.debug(f"Análise (ID: {analysis_node_id}) ligada à Empresa: {company_name} (ID: {comp['id']})")

                for sector_name in analysis_data["identified_entities"]["sectors"]:
                    merge_activity_query = """
                    MERGE (ea:EconomicActivity {name: $sector_name_param})
                    ON CREATE SET ea.type = 'Sector Focus', ea.created_at = datetime()
                    ON MATCH SET ea.last_referenced_at = datetime()
                    RETURN elementId(ea) as activity_id
                    """ # Renomeado $sector_name para $sector_name_param para evitar conflito se parameters passasse tudo
                    activity_query_result = self.neo4j_conn.execute_query(merge_activity_query, {"sector_name_param": sector_name})
                    if not activity_query_result or not activity_query_result[0] or "activity_id" not in activity_query_result[0]:
                        logger.error(f"Falha ao criar/mesclar nó EconomicActivity para setor '{sector_name}'")
                        continue
                    activity_id = activity_query_result[0]["activity_id"]

                    link_analysis_activity_query = """
                    MATCH (a:Analysis), (ea:EconomicActivity)
                    WHERE elementId(a) = $analysis_id AND elementId(ea) = $activity_id
                    MERGE (a)-[:ANALYZES_ACTIVITY {activity_type: 'Sector Focus'}]->(ea)
                    """
                    self.neo4j_conn.execute_query(link_analysis_activity_query, {"analysis_id": analysis_node_id, "activity_id": activity_id})
                    logger.debug(f"Análise (ID: {analysis_node_id}) ligada à EconomicActivity (Setor): {sector_name} (ID: {activity_id})")
                        
        except Exception as e:
            logger.error(f"Erro ao salvar análise agregada no Neo4j para notícia hash {news_hash}: {e}", exc_info=True)

    def process_news_batch(self, news_batch: List[Dict[str, str]]) -> List[Optional[Dict[str, Any]]]:
        results = []
        processed_news_hashes: Set[int] = set() 
        for news_item in news_batch: 
            result = self.process_news_item(news_item, processed_news_hashes)
            if result: 
                results.append(result)
        return results