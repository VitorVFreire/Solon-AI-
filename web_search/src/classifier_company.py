import pandas as pd
import os
from typing import List, Dict, Any, Tuple
import re
import json
from langchain_core.documents import Document
from langgraph.graph import StateGraph, END
from tqdm import tqdm
from utils import clean_filename

# Classe para gerenciar o RAG com as empresass
class EmpresasRAG:
    def __init__(self, empresas_data: List[Dict[str, Any]], max_context=15):
        self.empresas_data = empresas_data
        self.max_context = max_context
        self.setup_rag()
    
    def setup_rag(self):
        documents = []
        for empresas in self.empresas_data:
            company_name = empresas.get('name')
            company_full_name = empresas.get('full_name')
            country = empresas.get('country')
            symbol = empresas.get('symbol')
            
            if not company_name or not company_full_name:
                tqdm.write(f"Aviso: empresas inválida - {empresas}")
                continue
            
            # Concatenar as informações para o conteúdo do documento
            content = f"{company_name} ({company_full_name}), {country}, {symbol}"
            metadata = {
                "company_name": company_name,
                "company_full_name": company_full_name,
                "country": country,
                "symbol": symbol
            }
            documents.append(Document(page_content=content, metadata=metadata))
        
        class EnhancedTextSimilarity:
            def __init__(self, documents):
                self.documents = documents
                
            def similarity_search(self, query, k=5):
                def enhanced_similarity(doc, query):
                    doc_words = set(re.sub(r'[^\w\s]', ' ', doc.page_content.lower()).split())
                    query_words = set(re.sub(r'[^\w\s]', ' ', query.lower()).split())
                    company_bonus = 0
                    if hasattr(doc, 'metadata') and 'company_name' in doc.metadata:
                        if doc.metadata['company_name'].lower() in query.lower():
                            company_bonus = 0.3
                    country_bonus = 0
                    if hasattr(doc, 'metadata') and 'country' in doc.metadata:
                        if doc.metadata['country'].lower() in query.lower():
                            country_bonus = 0.1
                    common_words = doc_words.intersection(query_words)
                    base_score = len(common_words) / max(len(doc_words), len(query_words), 1)
                    return base_score + company_bonus + country_bonus
                
                similarities = [(doc, enhanced_similarity(doc, query)) for doc in self.documents]
                similarities.sort(key=lambda x: x[1], reverse=True)
                return [doc for doc, _ in similarities[:min(k, len(similarities))]]
        
        self.vectorstore = EnhancedTextSimilarity(documents)
    
    def query_similar_activities(self, query: str) -> List[Document]:
        return self.vectorstore.similarity_search(query, k=self.max_context)

# Classe para processar as empresass
class EmpresassProcessor:
    def __init__(self, rag_system: EmpresasRAG, llm_client, system_prompt_file: str, human_prompt_file: str, list_activities: list,output_dir: str = "resultados"):
        self.rag_system = rag_system
        self.llm_client = llm_client
        self.output_dir = output_dir
        self.list_activities = list_activities
        self.system_prompt = open(system_prompt_file).read().replace("{atividades}", ", ".join(self.list_activities))
        self.human_prompt = open(human_prompt_file).read()
        os.makedirs(self.output_dir, exist_ok=True)
        self.build_graph()
    
    def retrieve(self, state: Dict[str, Any]) -> Dict[str, Any]:
        query = f"{state['company_name']} {state['country']}"
        context_docs = self.rag_system.query_similar_activities(query)
        context = "\n".join([doc.page_content for doc in context_docs])
        state["context"] = context
        return state

    def generate_dependencies(self, state: Dict[str, Any]) -> Dict[str, Any]:
        input_data = {
            "system": self.system_prompt,
            "human": self.human_prompt.format(
                company_name=state['company_name'],
                company_full_name=state['company_full_name'],
                country=state['country'],
                symbol=state['symbol']
            )
        }
        state["result"] = self.llm_client.invoke(input_data)
        return state
    
    def format_output(self, state: Dict[str, Any]) -> Dict[str, Any]:
        try:
            json_result = json.loads(state["result"])
        except json.JSONDecodeError:
            json_result = {
                "error": "Não foi possível parsear a resposta como JSON",
                "raw_response": state["result"]
            }
        
        # Salvar o JSON em um arquivo
        clean_name = clean_filename(state['company_name'])
        output_path = os.path.join(self.output_dir, f"{clean_name}.json")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(json_result, f, ensure_ascii=False, indent=2)
        
        state["formatted_result"] = pd.DataFrame([json_result])
        return state
    
    def build_graph(self):
        from typing import TypedDict
        class empresasState(TypedDict):
            company_name: str
            company_full_name: str
            country: str
            symbol: str
            context: str
            result: str
            formatted_result: pd.DataFrame
        
        workflow = StateGraph(empresasState)
        workflow.add_node("retrieve", self.retrieve)
        workflow.add_node("generate_dependencies", self.generate_dependencies)
        workflow.add_node("format_output", self.format_output)
        workflow.set_entry_point("retrieve")
        workflow.add_edge("retrieve", "generate_dependencies")
        workflow.add_edge("generate_dependencies", "format_output")
        workflow.add_edge("format_output", END)
        self.graph = workflow.compile()
    
    def process_company(self, company_data):
        # Prepara o estado inicial com os dados da empresa
        state = {
            "company_name": company_data.get("name", ""),
            "company_full_name": company_data.get("full_name", ""),
            "country": company_data.get("country", ""),
            "symbol": company_data.get("symbol", "")
        }
        
        # Invoca o grafo com o estado preparado
        return self.graph.invoke(state)