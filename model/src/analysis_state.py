import pandas as pd
import os
from typing import List, Dict, Any, TypedDict, Optional
import json
from langgraph.graph import StateGraph, END
from tqdm import tqdm
import logging
import datetime
from utils import clean_filename
from src import Neo4jConnection

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NewsProcessor:
    def __init__(self, llm_client, system_prompt_file: str, human_prompt_file: str, 
                 neo4j_connection: Optional[Neo4jConnection] = None, 
                 output_dir: str = "resultados/news"):
        self.llm_client = llm_client
        self.output_dir = output_dir
        self.neo4j_connection = neo4j_connection
        self.system_prompt = open(system_prompt_file, encoding='utf-8').read()
        self.human_prompt = open(human_prompt_file, encoding='utf-8').read()
        os.makedirs(self.output_dir, exist_ok=True)
        self.workflow = self._create_workflow()

    def _create_workflow(self) -> StateGraph:
        class NewsAnalysisState(TypedDict, total=False):
            company_id: Optional[int]
            company_name: Optional[str]
            economic_activity: Optional[str]
            perfil: str
            news: str
            result: Optional[str]
            formatted_result: Optional[Any]
            affected_companies: List[Dict[str, Any]]
            cascade_completed: bool
            
        builder = StateGraph(state_schema=NewsAnalysisState)
        builder.add_node("identify_entities", self.identify_entities)
        builder.add_node("generate_analysis", self.generate_analysis)
        builder.add_node("format_output", self.format_output)
        builder.add_node("process_cascade_impact", self.process_cascade_impact)
        
        builder.set_entry_point("identify_entities")
        builder.add_edge("identify_entities", "generate_analysis")
        builder.add_edge("generate_analysis", "format_output")
        builder.add_edge("format_output", "process_cascade_impact")
        builder.add_conditional_edges(
            "process_cascade_impact",
            lambda state: "process_cascade_impact" if not state.get("cascade_completed", False) else END
        )
        
        return builder.compile()

    def identify_entities(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Identifica empresas conhecidas ou atividades econômicas no artigo."""
        if not self.neo4j_connection:
            # Se não houver conexão com Neo4j, tentar identificar atividades econômicas
            economic_activity = self._identify_economic_activity(state['news'])
            state["economic_activity"] = economic_activity
            return state

        # Buscar empresas no Neo4j que correspondem ao conteúdo da notícia
        query = """
        MATCH (c:Company)
        WHERE toLower(c.company_name) CONTAINS toLower($search_text)
        RETURN id(c) as id, c.company_name as company_name
        LIMIT 1
        """
        
        news_text = state['news'][:1000]  # Limitar tamanho do texto para busca
        with self.neo4j_connection.driver.session() as session:
            result = session.run(query, search_text=news_text)
            record = result.single()
            
            if record:
                state["company_id"] = record["id"]
                state["company_name"] = record["company_name"]
            else:
                # Se nenhuma empresa for encontrada, identificar atividade econômica
                state["economic_activity"] = self._identify_economic_activity(state['news'])
        
        return state

    def _identify_economic_activity(self, news_text: str) -> str:
        """Identifica a atividade econômica principal mencionada na notícia."""
        # Lista simplificada de atividades econômicas
        economic_activities = {
            "tecnologia": ["tecnologia", "software", "ti ", "informática", "tecnológico"],
            "indústria": ["indústria", "manufatura", "produção", "fábrica"],
            "varejo": ["varejo", "comércio", "loja", "supermercado"],
            "financeiro": ["banco", "financeiro", "investimento", "crédito"],
            "saúde": ["saúde", "hospital", "clínica", "médico"],
            # Adicionar mais setores conforme necessário
        }

        news_text_lower = news_text.lower()
        for activity, keywords in economic_activities.items():
            if any(keyword in news_text_lower for keyword in keywords):
                return activity
        
        return "outros"  # Default para atividades não identificadas

    def generate_analysis(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Gera análise a partir de notícias."""
        input_data = {
            "system": self.system_prompt,
            "human": self.human_prompt.format(
                perfil=state['perfil'],
                news=state['news'],
                company_name=state.get('company_name', 'N/A'),
                economic_activity=state.get('economic_activity', 'N/A')
            )
        }
        state["result"] = self.llm_client.invoke(input_data)
        return state
    
    def format_output(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Formata a saída da análise."""
        try:
            json_result = json.loads(state["result"])
            json_result["company_name"] = state.get("company_name", "N/A")
            json_result["economic_activity"] = state.get("economic_activity", "N/A")
            json_result["analyzed_at"] = datetime.datetime.now().isoformat()
            json_result["news_snippet"] = state["news"][:200] + "..." if len(state["news"]) > 200 else state["news"]
            
            if state.get("company_id"):
                json_result["company_id"] = state["company_id"]
            
        except json.JSONDecodeError:
            json_result = {
                "error": "Não foi possível parsear a resposta como JSON",
                "raw_response": state["result"],
                "company_name": state.get("company_name", "N/A"),
                "economic_activity": state.get("economic_activity", "N/A")
            }
        
        output_filename = f"{clean_filename(state.get('company_name', state.get('economic_activity', 'unknown')))}_analysis.json"
        output_path = os.path.join(self.output_dir, output_filename)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(json_result, f, ensure_ascii=False, indent=2)
        
        state["formatted_result"] = json_result
        state["affected_companies"] = state.get("affected_companies", [])
        state["cascade_completed"] = False
        return state
    
    def process_cascade_impact(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Processa impacto em cascata para empresas relacionadas."""
        if not self.neo4j_connection or not state.get("company_id"):
            state["cascade_completed"] = True
            logger.info("Processamento em cascata ignorado: conexão Neo4j ausente ou ID da empresa não fornecido")
            return state
        
        sector_score = state.get("formatted_result", {}).get("sector_score", 0)
        if sector_score > 3.0:
            try:
                related_companies = self._find_related_companies(state["company_id"])
                for company in related_companies:
                    dependency_level = company.get("dependency_level", 0.5)
                    cascade_impact = sector_score * dependency_level
                    
                    if cascade_impact > 1.5:
                        impact_data = {
                            "company_id": company["id"],
                            "company_name": company["company_name"],
                            "original_impact": sector_score,
                            "cascade_impact": cascade_impact,
                            "dependency_level": dependency_level,
                            "source_company": state["company_name"]
                        }
                        state["affected_companies"].append(impact_data)
                
                logger.info(f"Processamento em cascata: encontradas {len(state['affected_companies'])} empresas afetadas")
                
                cascade_output_path = os.path.join(
                    self.output_dir, 
                    f"cascade_impact_{clean_filename(state.get('company_name', 'unknown'))}.json"
                )
                with open(cascade_output_path, 'w', encoding='utf-8') as f:
                    json.dump({
                        "source_company": state.get("company_name", "N/A"),
                        "source_score": sector_score,
                        "affected_companies": state["affected_companies"]
                    }, f, ensure_ascii=False, indent=2)
                
            except Exception as e:
                logger.error(f"Erro ao processar impacto em cascata: {str(e)}")
        
        state["cascade_completed"] = True
        return state
    
    def _find_related_companies(self, company_id: int) -> List[Dict[str, Any]]:
        """Encontra empresas relacionadas no Neo4j."""
        query = """
        MATCH (a:Company)-[r]-(b:Company)
        WHERE id(a) = $company_id
        RETURN id(b) as id, b.company_name as company_name, 
               r.dependency_level as dependency_level, 
               r.correlation_level as correlation_level,
               type(r) as relationship_type
        """
        
        with self.neo4j_connection.driver.session() as session:
            result = session.run(query, company_id=company_id)
            return [dict(record) for record in result]
    
    def process_news_batch(self, news_array: List[Dict[str, str]], perfil: str = "Moderado") -> List[Dict[str, Any]]:
        """Processa um lote de notícias."""
        results = []
        
        for news_item in tqdm(news_array, desc="Processando notícias", unit="notícia"):
            try:
                if not all(key in news_item for key in ["title", "article"]):
                    logger.warning("Notícia inválida: faltando title ou article")
                    continue
                
                news_content = f"{news_item['title']}\n\n{news_item['article']}"
                state = {
                    "perfil": perfil,
                    "news": news_content
                }
                
                result = self.workflow.invoke(state)
                results.append(result)
                
                tqdm.write(f"Processada notícia - Score: {result.get('formatted_result', {}).get('sector_score', 'N/A')}")
                
            except Exception as e:
                logger.error(f"Erro ao processar notícia: {str(e)}")
                results.append({"error": str(e), "news_snippet": news_content[:200] + "..."})
        
        summary = {
            "total_news": len(news_array),
            "processed_news": len([r for r in results if "error" not in r]),
            "processing_date": datetime.datetime.now().isoformat()
        }
        
        summary_path = os.path.join(self.output_dir, "processing_summary.json")
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        return results