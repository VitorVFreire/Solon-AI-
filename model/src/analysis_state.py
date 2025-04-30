import pandas as pd
import os
from typing import List, Dict, Any, Tuple, TypedDict, Optional
import re
import json
from langgraph.graph import StateGraph, END
from tqdm import tqdm
import logging
import datetime
import requests
from utils import clean_filename
from graph.src.neo4j_connection import Neo4jConnection
from src.news_service import NewsService

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NewsProcessor:
    def __init__(self, llm_client, system_prompt_file: str, human_prompt_file: str, 
                 neo4j_connection: Optional[Neo4jConnection] = None, 
                 news_service: Optional[NewsService] = None,
                 output_dir: str = "resultados/news"):
        """
        Inicializa o processador de notícias.
        
        Args:
            llm_client: Cliente LLM para processamento
            system_prompt_file: Caminho para o arquivo de prompt do sistema
            human_prompt_file: Caminho para o arquivo de prompt do usuário
            neo4j_connection: Conexão com o banco de dados Neo4j
            news_service: Serviço para busca de notícias
            output_dir: Diretório para salvar os resultados
        """
        self.llm_client = llm_client
        self.output_dir = output_dir
        self.neo4j_connection = neo4j_connection
        
        # Inicializar serviço de notícias
        self.news_service = news_service if news_service else NewsService()
        
        # Carregar prompts
        self.system_prompt = open(system_prompt_file, encoding='utf-8').read()
        self.human_prompt = open(human_prompt_file, encoding='utf-8').read()
        
        # Criar diretório de saída
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Inicializa o grafo de processamento
        self.workflow = self._create_workflow()

    def _create_workflow(self) -> StateGraph:
        """Cria o fluxo de trabalho usando LangGraph."""
        # Definir o grafo com schema para o estado
        from typing import TypedDict, Optional, List, Dict, Any

        class NewsAnalysisState(TypedDict, total=False):
            company_id: Optional[int]
            company_name: str
            data_limite: str
            perfil: str
            news: str
            result: Optional[str]
            formatted_result: Optional[Any]
            affected_companies: List[Dict[str, Any]]
            cascade_completed: bool
            
        # Definir o grafo
        builder = StateGraph(state_schema=NewsAnalysisState)
        
        # Adicionar os nós
        builder.add_node("generate_analysis", self.generate_analysis)
        builder.add_node("format_output", self.format_output)
        builder.add_node("process_cascade_impact", self.process_cascade_impact)
        
        # Configurar as arestas com ponto de entrada
        builder.set_entry_point("generate_analysis")
        builder.add_edge("generate_analysis", "format_output")
        
        # Adicionar lógica condicional para processar impacto em cascata
        builder.add_edge("format_output", "process_cascade_impact")
        builder.add_conditional_edges(
            "process_cascade_impact",
            lambda state: "process_cascade_impact" if not state.get("cascade_completed", False) else END
        )
        
        # Compilar o grafo
        return builder.compile()

    def generate_analysis(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Gera uma análise a partir de notícias.
        
        Args:
            state: Estado atual do processamento
            
        Returns:
            Estado atualizado com o resultado da análise
        """
        input_data = {
            "system": self.system_prompt,
            "human": self.human_prompt.format(
                data_limite=state['data_limite'],
                news=state['news'],
                perfil=state['perfil']
            )
        }
        state["result"] = self.llm_client.invoke(input_data)
        return state
    
    def format_output(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Formata a saída da análise.
        
        Args:
            state: Estado atual do processamento
            
        Returns:
            Estado atualizado com o resultado formatado
        """
        try:
            json_result = json.loads(state["result"])
            
            # Adicionar metadados ao resultado
            json_result["company_name"] = state["company_name"]
            json_result["analyzed_at"] = datetime.datetime.now().isoformat()
            json_result["news_snippet"] = state["news"][:200] + "..." if len(state["news"]) > 200 else state["news"]
            
            if state.get("company_id"):
                json_result["company_id"] = state["company_id"]
            
        except json.JSONDecodeError:
            json_result = {
                "error": "Não foi possível parsear a resposta como JSON",
                "raw_response": state["result"],
                "company_name": state["company_name"]
            }
        
        # Salvar o JSON em um arquivo
        output_path = os.path.join(self.output_dir, f"{clean_filename(state['company_name'])}_analysis.json")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(json_result, f, ensure_ascii=False, indent=2)
        
        state["formatted_result"] = json_result
        
        # Inicializar lista de empresas afetadas
        if not state.get("affected_companies"):
            state["affected_companies"] = []
            
        # Definir se o processamento em cascata foi concluído
        state["cascade_completed"] = False
        
        return state
    
    def process_cascade_impact(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Processa o impacto em cascata para empresas relacionadas.
        
        Args:
            state: Estado atual do processamento
            
        Returns:
            Estado atualizado com informações sobre empresas afetadas
        """
        if not self.neo4j_connection or not state.get("company_id"):
            # Se não houver conexão com Neo4j ou ID da empresa, marcar como concluído
            state["cascade_completed"] = True
            logger.info("Processamento em cascata ignorado: conexão Neo4j ausente ou ID da empresa não fornecido")
            return state
        
        # Obter pontuação do impacto setorial para cascata
        sector_score = state.get("formatted_result", {}).get("sector_score", 0)
        
        # Se o impacto setorial for significativo (> 3.0), buscar empresas relacionadas
        if sector_score > 3.0:
            try:
                # Encontrar empresas relacionadas no Neo4j
                related_companies = self._find_related_companies(state["company_id"])
                
                # Adicionar as empresas afetadas ao estado
                for company in related_companies:
                    # Calcular o impacto na empresa relacionada com base no nível de dependência
                    dependency_level = company.get("dependency_level", 0.5)
                    cascade_impact = sector_score * dependency_level
                    
                    # Só considerar impactos significativos (> 1.5)
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
                
                # Salvar o resultado do impacto em cascata
                cascade_output_path = os.path.join(
                    self.output_dir, 
                    f"cascade_impact_{clean_filename(state['company_name'])}.json"
                )
                with open(cascade_output_path, 'w', encoding='utf-8') as f:
                    json.dump({
                        "source_company": state["company_name"],
                        "source_score": sector_score,
                        "affected_companies": state["affected_companies"]
                    }, f, ensure_ascii=False, indent=2)
                
            except Exception as e:
                logger.error(f"Erro ao processar impacto em cascata: {str(e)}")
        
        # Marcar o processamento em cascata como concluído
        state["cascade_completed"] = True
        return state
    
    def _find_related_companies(self, company_id: int) -> List[Dict[str, Any]]:
        """
        Encontra empresas relacionadas no Neo4j.
        
        Args:
            company_id: ID da empresa no Neo4j
            
        Returns:
            Lista de empresas relacionadas
        """
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
    
    def fetch_news_for_company(self, company_name: str, limit: int = 5, language: str = "pt", days_back: int = 7) -> List[Dict[str, Any]]:
        """
        Busca notícias para uma empresa usando o serviço de notícias.
        
        Args:
            company_name: Nome da empresa
            limit: Número máximo de notícias a retornar
            language: Idioma das notícias
            days_back: Quantidade de dias para buscar notícias anteriores
            
        Returns:
            Lista de notícias
        """
        try:
            logger.info(f"Buscando notícias para {company_name}")
            news_list = self.news_service.search_news(
                query=company_name,
                limit=limit,
                language=language,
                days_back=days_back
            )
            
            # Salvar as notícias encontradas
            if news_list:
                news_dir = os.path.join(self.output_dir, "raw_news")
                self.news_service.save_news_to_file(news_list, company_name, news_dir)
                logger.info(f"Encontradas {len(news_list)} notícias para {company_name}")
            else:
                logger.warning(f"Nenhuma notícia encontrada para {company_name}")
                
            return news_list
                
        except Exception as e:
            logger.error(f"Erro ao buscar notícias para {company_name}: {str(e)}")
            return []
    
    def process_companies_from_neo4j(self, perfil: str = "Moderado", data_limite: str = None):
        """
        Processa todas as empresas do Neo4j, buscando notícias e analisando.
        
        Args:
            perfil: Perfil do investidor para análise
            data_limite: Data limite para o conhecimento (se None, usa a data atual)
            
        Returns:
            Dict com resumo do processamento
        """
        if not self.neo4j_connection:
            raise ValueError("Conexão com Neo4j não fornecida")
        
        if not data_limite:
            data_limite = datetime.datetime.now().strftime("%d/%m/%Y")
        
        # Buscar todas as empresas no Neo4j
        query = "MATCH (c:Company) RETURN id(c) as id, c.company_name as company_name"
        
        companies = []
        with self.neo4j_connection.driver.session() as session:
            result = session.run(query)
            companies = [dict(record) for record in result]
        
        total_companies = len(companies)
        processed_companies = 0
        companies_with_news = 0
        
        # Processar cada empresa
        for company in tqdm(companies, desc="Processando empresas", unit="empresa"):
            try:
                company_id = company["id"]
                company_name = company["company_name"]
                
                # Buscar notícias para a empresa
                news_list = self.fetch_news_for_company(company_name)
                
                if not news_list:
                    logger.info(f"Nenhuma notícia encontrada para {company_name}")
                    continue
                
                companies_with_news += 1
                
                # Processar cada notícia
                for news_item in news_list:
                    news_content = f"{news_item['title']}\n\n{news_item['content']}"
                    
                    # Preparar estado inicial
                    state = {
                        "company_id": company_id,
                        "company_name": company_name,
                        "data_limite": data_limite,
                        "perfil": perfil,
                        "news": news_content
                    }
                    
                    # Executar o fluxo de trabalho
                    result = self.workflow.invoke(state)
                    
                    tqdm.write(f"Processada notícia para {company_name} - Score: {result.get('formatted_result', {}).get('sector_score', 'N/A')}")
                
                processed_companies += 1
                
            except Exception as e:
                logger.error(f"Erro ao processar empresa {company.get('company_name', 'desconhecida')}: {str(e)}")
        
        summary = {
            "total_companies": total_companies,
            "processed_companies": processed_companies,
            "companies_with_news": companies_with_news,
            "processing_date": datetime.datetime.now().isoformat()
        }
        
        # Salvar resumo
        summary_path = os.path.join(self.output_dir, "processing_summary.json")
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        return summary
    
    def process_news(self, news_data, company_name=None, company_id=None):
        """
        Processa uma única notícia.
        
        Args:
            news_data: Dicionário contendo 'data_limite', 'perfil', e 'news'
            company_name: Nome da empresa (opcional)
            company_id: ID da empresa no Neo4j (opcional)
            
        Returns:
            O estado final após o processamento completo
        """
        # Prepara o estado inicial
        state = {
            "data_limite": news_data.get("data_limite", ""),
            "perfil": news_data.get("perfil", ""),
            "news": news_data.get("news", "")
        }
        
        if company_name:
            state["company_name"] = company_name
        else:
            state["company_name"] = "unknown_company"
            
        if company_id:
            state["company_id"] = company_id
        
        try:
            # Invoca o grafo com o estado preparado
            result = self.workflow.invoke(state)
            return result
        except Exception as e:
            logger.error(f"Erro ao processar notícias para {state['company_name']}: {str(e)}")
            # Executa os passos de forma sequencial se o grafo falhar
            fallback_state = self.generate_analysis(state.copy())
            intermediate_state = self.format_output(fallback_state)
            final_state = self.process_cascade_impact(intermediate_state)
            return final_state