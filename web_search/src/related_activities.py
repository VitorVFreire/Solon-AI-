import os
import re
import json
import pandas as pd
from typing import List, Dict, Any, TypedDict
from langchain_core.documents import Document
from langgraph.graph import StateGraph, END
from tqdm import tqdm
from utils import clean_filename

class ActivitiesRAG:
    def __init__(self, activities_data: List[Dict[str, Any]], max_context: int = 15):
        self.activities_data = activities_data
        self.max_context = max_context
        self.setup_rag()
    
    def setup_rag(self):
        documents = []
        for atividade in self.activities_data:
            atividade_economica = atividade.get('atividade_economica')
            descricao_atividade = atividade.get('descricao_atividade')
            tipo_atividade = atividade.get('tipo_atividade')
            nivel_importancia = atividade.get('nivel_importancia')
            
            if not atividade_economica or not descricao_atividade:
                tqdm.write(f"Aviso: Atividade inválida - {atividade}")
                continue
            
            content = f"{atividade_economica} ({descricao_atividade}), {tipo_atividade}, {nivel_importancia}"
            metadata = {
                "atividade_economica": atividade_economica,
                "descricao_atividade": descricao_atividade,
                "tipo_atividade": tipo_atividade,
                "nivel_importancia": nivel_importancia
            }
            documents.append(Document(page_content=content, metadata=metadata))
        
        class EnhancedTextSimilarity:
            def __init__(self, documents: List[Document]):
                self.documents = documents
                
            def similarity_search(self, query: str, k: int = 5) -> List[Document]:
                def enhanced_similarity(doc: Document, query: str) -> float:
                    doc_words = set(re.sub(r'[^\w\s]', ' ', doc.page_content.lower()).split())
                    query_words = set(re.sub(r'[^\w\s]', ' ', query.lower()).split())
                    atividade_bonus = 0.3 if doc.metadata['atividade_economica'].lower() in query.lower() else 0
                    nivel_importancia_bonus = 0.1 if str(doc.metadata['nivel_importancia']) in query.lower() else 0
                    common_words = doc_words.intersection(query_words)
                    base_score = len(common_words) / max(len(doc_words), len(query_words), 1)
                    return base_score + atividade_bonus + nivel_importancia_bonus
                
                similarities = [(doc, enhanced_similarity(doc, query)) for doc in self.documents]
                similarities.sort(key=lambda x: x[1], reverse=True)
                return [doc for doc, _ in similarities[:min(k, len(similarities))]]
        
        self.vectorstore = EnhancedTextSimilarity(documents)
    
    def query_similar_activities(self, query: str) -> List[Document]:
        return self.vectorstore.similarity_search(query, k=self.max_context)

class ActivitiesProcessor:
    def __init__(self, rag_system: ActivitiesRAG, llm_client: Any, system_prompt_file: str, 
                 human_prompt_file: str, list_activities: List[str], output_dir: str = "resultados"):
        self.rag_system = rag_system
        self.llm_client = llm_client
        self.output_dir = output_dir
        self.list_activities = list_activities
        
        if not os.path.exists(system_prompt_file):
            raise FileNotFoundError(f"Arquivo de system prompt não encontrado: {system_prompt_file}")
        if not os.path.exists(human_prompt_file):
            raise FileNotFoundError(f"Arquivo de human prompt não encontrado: {human_prompt_file}")
        
        self.system_prompt = open(system_prompt_file, encoding='utf-8').read().replace(
            "{activities}", ", ".join(self.list_activities)
        )
        self.human_prompt = open(human_prompt_file, encoding='utf-8').read()
        os.makedirs(self.output_dir, exist_ok=True)
        self.build_graph()
    
    def retrieve(self, state: Dict[str, Any]) -> Dict[str, Any]:
        query = f"{state['atividade_economica']} {state['nivel_importancia']}"
        context_docs = self.rag_system.query_similar_activities(query)
        context = "\n".join([doc.page_content for doc in context_docs])
        state["context"] = context
        return state

    def generate_dependencies(self, state: Dict[str, Any]) -> Dict[str, Any]:
        input_data = {
            "system": self.system_prompt,
            "human": self.human_prompt.format(
                atividade_economica=state['atividade_economica'],
                descricao_atividade=state['descricao_atividade'],
                tipo_atividade=state['tipo_atividade'],
                nivel_importancia=state['nivel_importancia']
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
        
        clean_name = clean_filename(state['atividade_economica'])
        output_path = os.path.join(self.output_dir, f"{clean_name}.json")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(json_result, f, ensure_ascii=False, indent=2)
        
        state["formatted_result"] = pd.DataFrame([json_result])
        return state
    
    def build_graph(self):
        class AtividadeState(TypedDict):
            atividade_economica: str
            descricao_atividade: str
            tipo_atividade: str
            nivel_importancia: str
            context: str
            result: str
            formatted_result: pd.DataFrame
        
        workflow = StateGraph(AtividadeState)
        workflow.add_node("retrieve", self.retrieve)
        workflow.add_node("generate_dependencies", self.generate_dependencies)
        workflow.add_node("format_output", self.format_output)
        workflow.set_entry_point("retrieve")
        workflow.add_edge("retrieve", "generate_dependencies")
        workflow.add_edge("generate_dependencies", "format_output")
        workflow.add_edge("format_output", END)
        self.graph = workflow.compile()
    
    def process_activities(self, activities_data: Dict[str, Any]) -> Dict[str, Any]:
        state = {
            "atividade_economica": activities_data.get("atividade_economica", ""),
            "descricao_atividade": activities_data.get("descricao_atividade", ""),
            "tipo_atividade": activities_data.get("tipo_atividade", ""),
            "nivel_importancia": str(activities_data.get("nivel_importancia", ""))
        }
        return self.graph.invoke(state)