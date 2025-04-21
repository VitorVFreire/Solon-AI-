import pandas as pd
import requests
import sqlite3
import os
from typing import List, Dict, Any, Tuple
from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.documents import Document
from langgraph.graph import END, StateGraph
import re

# Carrega variáveis de ambiente
load_dotenv()

# Configuração da API do xAI
def setup_xai_client():
    api_key = os.getenv("XAI_API_KEY")
    if not api_key:
        raise ValueError("XAI_API_KEY não encontrada nas variáveis de ambiente")
    return {
        "api_key": api_key,
        "base_url": "https://api.x.ai/v1",
        "model": "grok-3"
    }

# Classe para interação com a API do xAI
class XAIClient:
    def __init__(self, config: Dict[str, str]):
        self.api_key = config["api_key"]
        self.base_url = config["base_url"]
        self.model = config["model"]
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
    
    def invoke(self, input_data: Dict[str, Any]) -> str:
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": input_data["system"]},
                {"role": "user", "content": input_data["human"]}
            ],
            "max_tokens": 4000,
            "temperature": 0.3
        }
        try:
            response = requests.post(
                f"{self.base_url}/chat/completions",
                headers=self.headers,
                json=payload
            )
            response.raise_for_status()
            return response.json()["choices"][0]["message"]["content"]
        except Exception as e:
            raise Exception(f"Erro ao chamar a API do xAI: {str(e)}")

# Função para buscar o nome da atividade a partir do código no contexto
def get_activity_name_from_context(activity_code: str, context: str) -> str:
    lines = context.strip().split('\n')
    for line in lines:
        if ':' in line:
            parts = line.split(':', 1)
            if len(parts) == 2:
                code = parts[0].strip()
                description = parts[1].split(' [Setor:')[0].strip()
                if code == activity_code:
                    return description
    return activity_code

# Classe para gerenciar o RAG com as atividades
class AtividadesRAG:
    def __init__(self, atividades_data: List[Dict[str, Any]], max_context=15):
        self.atividades_data = atividades_data
        self.max_context = max_context
        self.setup_rag()
    
    def setup_rag(self):
        documents = []
        for atividade in self.atividades_data:
            numero = atividade.get('numero') or atividade.get('codigo_atividade')
            descricao = atividade.get('descricao') or atividade.get('nome_atividade')
            if not numero or not descricao:
                print(f"Aviso: Atividade inválida - {atividade}")
                continue
            content = f"{numero}:{descricao}"
            metadata = {
                "numero": numero, 
                "descricao": descricao,
                "categoria": atividade.get('categoria', ''),
                "subcategoria": atividade.get('subcategoria', '')
            }
            documents.append(Document(page_content=content, metadata=metadata))
        
        class EnhancedTextSimilarity:
            def __init__(self, documents):
                self.documents = documents
                
            def similarity_search(self, query, k=5):
                def enhanced_similarity(doc, query):
                    doc_words = set(re.sub(r'[^\w\s]', ' ', doc.page_content.lower()).split())
                    query_words = set(re.sub(r'[^\w\s]', ' ', query.lower()).split())
                    category_bonus = 0
                    if hasattr(doc, 'metadata') and 'categoria' in doc.metadata:
                        if doc.metadata['categoria'].lower() in query.lower():
                            category_bonus = 0.2
                    common_words = doc_words.intersection(query_words)
                    base_score = len(common_words) / max(len(doc_words), len(query_words), 1)
                    return base_score + category_bonus
                
                similarities = [(doc, enhanced_similarity(doc, query)) for doc in self.documents]
                similarities.sort(key=lambda x: x[1], reverse=True)
                return [doc for doc, _ in similarities[:min(k, len(similarities))]]
        
        self.vectorstore = EnhancedTextSimilarity(documents)
    
    def query_similar_activities(self, query: str) -> List[Document]:
        return self.vectorstore.similarity_search(query, k=self.max_context)

# Classe para o LangGraph que processa as atividades
class AtividadesProcessor:
    def __init__(self, rag_system: AtividadesRAG, llm_client, output_dir: str = "resultados", 
                max_output_atividades: int = 50, 
                tokens_por_fator: Dict[str, int] = None):
        self.rag_system = rag_system
        self.llm_client = llm_client
        self.output_dir = output_dir
        self.max_output_atividades = max_output_atividades
        self.tokens_por_fator = tokens_por_fator or {
            "cadeia_produtiva": 20,
            "financeira_mercado": 20,
            "estrutural_regulatoria": 20
        }
        self.atividade_mapping = {
            atividade.get('numero') or atividade.get('codigo_atividade'): 
            atividade.get('descricao') or atividade.get('nome_atividade')
            for atividade in rag_system.atividades_data
        }
        os.makedirs(self.output_dir, exist_ok=True)
        self.build_graph()
    
    def retrieve(self, state: Dict[str, Any]) -> Dict[str, Any]:
        query = state["atividade_financeira"]
        context_activities = []
        sectors_included = set()
        
        for atividade in self.rag_system.atividades_data:
            numero = atividade.get('numero') or atividade.get('codigo_atividade')
            descricao = atividade.get('descricao') or atividade.get('nome_atividade')
            categoria = atividade.get('categoria', '')
            
            if descricao != query:
                if len([a for a in context_activities if categoria in a]) < 3 or len(sectors_included) < 5:
                    sectors_included.add(categoria)
                    atividade_str = f"{numero}:{descricao}"
                    if categoria:
                        atividade_str += f" [Setor: {categoria}]"
                    context_activities.append(atividade_str)
        
        max_context = min(len(context_activities), self.max_output_atividades)
        context = context_activities[:max_context]
        state["context"] = "\n".join(context)
        return state
    
    def generate_dependencies(self, state: Dict[str, Any]) -> Dict[str, Any]:
        tokens_cadeia = self.tokens_por_fator.get("cadeia_produtiva", 20)
        tokens_financeira = self.tokens_por_fator.get("financeira_mercado", 20)
        tokens_estrutural = self.tokens_por_fator.get("estrutural_regulatoria", 20)
        
        system_prompt = f"""
        Você é um analista especializado em modelagem de dependências intersectoriais econômicas, com experiência em análise de redes complexas, propagação de impactos econômicos e avaliação de relevância financeira no mercado de capitais.

        TAREFA:
        Analise a atividade econômica principal fornecida e determine:
        1. O grau de dependência que cada atividade listada no contexto tem em relação à atividade principal, utilizando uma escala precisa de 0 a 5:
           - 0: Nenhuma dependência (setores economicamente independentes)
           - 1: Dependência muito baixa (interrupções causam efeitos mínimos)
           - 2: Dependência baixa (impactos limitados e de curto prazo)
           - 3: Dependência moderada (impactos significativos que exigem adaptação)
           - 4: Dependência alta (interrupções causam disfunções graves)
           - 5: Dependência muito alta (inviabilidade operacional sem a atividade principal)
        2. A relevância econômica ou popularidade de cada atividade no contexto de bolsa de valores, utilizando uma escala de 0 a 5:
           - 0: Nenhuma relevância (setor pouco conhecido ou irrelevante financeiramente)
           - 1: Relevância muito baixa (setor com pouca visibilidade ou impacto financeiro)
           - 2: Relevância baixa (setor com alguma presença, mas não significativo)
           - 3: Relevância moderada (setor com impacto financeiro notável ou empresas listadas)
           - 4: Relevância alta (setor com forte presença em bolsas ou alto valor de mercado)
           - 5: Relevância muito alta (setor crítico, com empresas líderes em bolsas globais)
           - Considere fatores como: presença de empresas do setor em bolsas de valores (ex.: B3, NYSE, NASDAQ), capitalização de mercado, volume de transações, ou popularidade em índices como Ibovespa, S&P 500.

        FATORES DE ANÁLISE (com controle de tokens):
        Para cada setor, avalie os seguintes fatores e forneça uma nota (0-5) e uma explicação breve:

        1. FATOR 1 - Integração da cadeia produtiva (máximo {tokens_cadeia} tokens):
           - Percentual de insumos diretos provenientes da atividade principal
           - Impossibilidade de substituição por fornecedores alternativos
           - Posição na cadeia (upstream/downstream)

        2. FATOR 2 - Interdependência financeira e de mercado (máximo {tokens_financeira} tokens):
           - Fluxos financeiros críticos entre os setores
           - Impacto de variações de preço da atividade principal
           - Base de clientes compartilhada
           - Canais de distribuição comuns

        3. FATOR 3 - Dependências estruturais e regulatórias (máximo {tokens_estrutural} tokens):
           - Infraestrutura física compartilhada
           - Sistemas tecnológicos integrados
           - Exposição a regulamentações compartilhadas

        RESULTADO:
        Forneça o resultado como uma tabela estruturada com as seguintes colunas:
        | Setor Econômico | Grau de Dependência | Coeficiente de Propagação | Relevância Econômica | Fator 1: Cadeia Produtiva | Fator 2: Financeira e Mercado | Fator 3: Estrutural e Regulatória |
        |-----------------|---------------------|---------------------------|----------------------|---------------------------|-------------------------------|----------------------------------|

        Para cada fator, inclua a nota (0-5) e uma explicação MUITO concisa dentro do limite de tokens, no formato:
        "Nota X: breve explicação"

        O Coeficiente de Propagação (0.0-1.0) indica quanto do impacto no setor principal será transmitido para o setor.

        IMPORTANTE:
        - Na coluna 'Setor Econômico', use SOMENTE o código da atividade (ex.: '01.11-3') SEM a descrição.
        - Inclua o máximo possível de atividades na análise.
        - Use explicações extremamente sucintas para cada fator (respeitando os limites de tokens).
        - Considere todos os setores no contexto e inclua apenas aqueles com algum grau de dependência mensurável (1-5).
        - NÃO inclua a atividade principal na análise ("Atividade Original").
        - Priorize atividades com maior relevância econômica ou popularidade na bolsa de valores.
        """
        
        human_prompt = """
        Atividade Principal para análise: {atividade_financeira}
        
        Lista de Atividades do Contexto:
        {context}
        
        Determine o grau de dependência (0-5), o coeficiente de propagação (0.0-1.0), e a relevância econômica ou popularidade no cenário de bolsa de valores (0-5) que cada atividade do contexto tem em relação à atividade principal "{atividade_financeira}", além de avaliar cada um dos 3 fatores separadamente.
        
        IMPORTANTE: 
        - Na coluna 'Setor Econômico', retorne SOMENTE o código da atividade (ex.: '01.11-3') SEM a descrição.
        - Priorize a quantidade de atividades analisadas. Avalie o máximo de atividades possível.
        - Para cada atividade, forneça os 3 fatores e a relevância econômica conforme solicitado, com notas e explicações muito concisas.
        - Use o formato "Nota X: breve explicação" para cada fator, respeitando o limite de tokens.
        - Inclua apenas atividades com grau de dependência maior que 0.
        - Não inclua a atividade principal ("{atividade_financeira}") na sua análise.
        - Ordene os resultados priorizando atividades com maior relevância econômica ou popularidade na bolsa de valores.
        
        Inclua na sua resposta uma tabela com todas as colunas solicitadas:
        | Setor Econômico | Grau de Dependência | Coeficiente de Propagação | Relevância Econômica | Fator 1: Cadeia Produtiva | Fator 2: Financeira e Mercado | Fator 3: Estrutural e Regulatória |
        """
        
        input_data = {
            "system": system_prompt,
            "human": human_prompt.format(
                atividade_financeira=state["atividade_financeira"],
                context=state["context"]
            )
        }
        state["result"] = self.llm_client.invoke(input_data)
        return state
    
    def format_output(self, state: Dict[str, Any]) -> Dict[str, Any]:
        result_text = state["result"]
        try:
            print("Raw API response:", result_text[:500])
            table_section = result_text
            lines = table_section.strip().split('\n')
            data = []
            
            for line in lines:
                if '|' in line:
                    parts = [p.strip() for p in line.split('|') if p.strip()]
                    if (len(parts) >= 7 and 
                        not all(c in '-:|' for c in parts[0]) and 
                        "Setor Econômico" not in parts[0] and 
                        parts[0] != ""):
                        try:
                            atividade_code = parts[0].strip()
                            atividade_name = get_activity_name_from_context(atividade_code, state["context"])
                            dependencia = int(parts[1].strip())
                            propagacao = float(parts[2].strip())
                            relevancia = int(parts[3].strip())
                            fator_cadeia = parts[4].strip()
                            fator_financeira = parts[5].strip()
                            fator_estrutural = parts[6].strip()
                            
                            # Obter categoria do contexto
                            categoria = ""
                            for ctx_line in state["context"].strip().split('\n'):
                                if atividade_code in ctx_line:
                                    if "[Setor:" in ctx_line:
                                        categoria = ctx_line.split("[Setor:")[1].strip("]").strip()
                                        break
                            
                            if 0 < dependencia <= 5:
                                data.append({
                                    "Atividade": atividade_name,
                                    "Grau de Dependência": dependencia,
                                    "Coeficiente de Propagação": propagacao,
                                    "Relevância Econômica": relevancia,
                                    "Fator 1: Cadeia Produtiva": fator_cadeia,
                                    "Fator 2: Financeira e Mercado": fator_financeira,
                                    "Fator 3: Estrutural e Regulatória": fator_estrutural,
                                    "Atividade Original": state["atividade_financeira"],
                                    "Categoria": categoria
                                })
                        except (ValueError, IndexError) as e:
                            print(f"Erro ao processar linha: {line} - {str(e)}")
                            continue
            
            result_df = pd.DataFrame(data)
            
            if not result_df.empty:
                # Ordenar por relevância econômica, grau de dependência e coeficiente de propagação
                result_df = result_df.sort_values(
                    by=["Relevância Econômica", "Grau de Dependência", "Coeficiente de Propagação"],
                    ascending=[False, False, False]
                )
                if len(result_df) > self.max_output_atividades:
                    result_df = result_df.head(self.max_output_atividades)
                state["formatted_result"] = result_df
                state["modelagem_propagacao"] = ""
            else:
                state["formatted_result"] = pd.DataFrame({
                    "Erro": ["Nenhuma dependência encontrada"],
                    "Resultado Original": [result_text]
                })
                state["modelagem_propagacao"] = ""
                
        except Exception as e:
            state["formatted_result"] = pd.DataFrame({
                "Erro": [f"Erro ao formatar: {str(e)}"],
                "Resultado Original": [result_text]
            })
            state["modelagem_propagacao"] = ""
            
        return state
    
    def save_to_excel(self, df: pd.DataFrame, atividade_financeira: str, modelagem_propagacao: str = ""):
        safe_atividade = atividade_financeira.replace("/", "_").replace("\\", "_").replace(":", "_")
        file_path = os.path.join(self.output_dir, f"resultado_{safe_atividade}.xlsx")
        
        if not df.empty and "Erro" not in df.columns:
            df["Atividade"] = df["Atividade"].map(self.atividade_mapping).fillna(df["Atividade"])
        
        with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
            if not df.empty and "Erro" not in df.columns:
                columns = [
                    "Atividade", 
                    "Grau de Dependência", 
                    "Coeficiente de Propagação",
                    "Relevância Econômica",
                    "Fator 1: Cadeia Produtiva", 
                    "Fator 2: Financeira e Mercado", 
                    "Fator 3: Estrutural e Regulatória",
                    "Atividade Original",
                    "Categoria"
                ]
                df = df[columns]
            
            df.to_excel(writer, sheet_name="Dependências", index=False)
            
            tokens_df = pd.DataFrame({
                "Fator": ["Cadeia Produtiva", "Financeira e Mercado", "Estrutural e Regulatória"],
                "Limite de Tokens": [
                    self.tokens_por_fator.get("cadeia_produtiva", 20),
                    self.tokens_por_fator.get("financeira_mercado", 20),
                    self.tokens_por_fator.get("estrutural_regulatoria", 20)
                ]
            })
            tokens_df.to_excel(writer, sheet_name="Configurações", index=False)
        
        print(f"Resultado salvo em: {file_path}")
        return file_path
    
    def build_graph(self):
        from typing import TypedDict
        class AtividadeState(TypedDict):
            atividade_financeira: str
            context: str
            result: str
            formatted_result: pd.DataFrame
            modelagem_propagacao: str
        
        workflow = StateGraph(AtividadeState)
        workflow.add_node("retrieve", self.retrieve)
        workflow.add_node("generate_dependencies", self.generate_dependencies)
        workflow.add_node("format_output", self.format_output)
        workflow.set_entry_point("retrieve")
        workflow.add_edge("retrieve", "generate_dependencies")
        workflow.add_edge("generate_dependencies", "format_output")
        workflow.add_edge("format_output", END)
        self.graph = workflow.compile()
    
    def process_activity(self, atividade_financeira: str) -> Tuple[pd.DataFrame, str, str]:
        initial_state = {"atividade_financeira": atividade_financeira}
        final_state = self.graph.invoke(initial_state)
        file_path = self.save_to_excel(
            final_state["formatted_result"], 
            atividade_financeira, 
            final_state.get("modelagem_propagacao", "")
        )
        return final_state["formatted_result"], final_state["result"], file_path

def clean_filename(filename):
    return filename.replace('/', '_').replace('\\\\', '_').replace(':', '_')

def main():
    try:
        load_dotenv()
        
        tokens_cadeia = int(os.getenv("TOKENS_CADEIA_PRODUTIVA", "20"))
        tokens_financeira = int(os.getenv("TOKENS_FINANCEIRA_MERCADO", "20"))
        tokens_estrutural = int(os.getenv("TOKENS_ESTRUTURAL_REGULATORIA", "20"))
        
        tokens_por_fator = {
            "cadeia_produtiva": tokens_cadeia,
            "financeira_mercado": tokens_financeira,
            "estrutural_regulatoria": tokens_estrutural
        }
        
        conn = sqlite3.connect("../data_scraper/cnpj.db")
        query = "SELECT codigo_atividade, nome_atividade FROM cnaes"
        df_atividades = pd.read_sql_query(query, conn)
        conn.close()
        
        atividades_exemplo = df_atividades.rename(columns={
            "codigo_atividade": "numero",
            "nome_atividade": "descricao"
        }).to_dict(orient="records")
        
        max_atividades = 120#len(atividades_exemplo)
        
        max_output_env = os.getenv("MAX_OUTPUT_ATIVIDADES")
        max_output_atividades = int(max_output_env) if max_output_env else max_atividades
        print(f"Total de atividades disponíveis: {max_atividades}")
        
        df_financeiras = pd.read_excel("atividades_analisadas/para_analise.xlsx")
        atividades_financeiras = df_financeiras["Atividades"].tolist()
        
        xai_config = setup_xai_client()
        xai_client = XAIClient(xai_config)
        
        print(f"Configurando sistema RAG com {len(atividades_exemplo)} atividades...")
        rag_system = AtividadesRAG(atividades_exemplo, max_context=max_atividades)
        
        print(f"Configurando processador de atividades:")
        print(f"- Limite de output: {max_output_atividades} atividades")
        print(f"- Tokens para Cadeia Produtiva: {tokens_cadeia}")
        print(f"- Tokens para Financeira e Mercado: {tokens_financeira}")
        print(f"- Tokens para Estrutural e Regulatória: {tokens_estrutural}")
        
        processor = AtividadesProcessor(
            rag_system, 
            xai_client, 
            output_dir="resultados", 
            max_output_atividades=max_output_atividades,
            tokens_por_fator=tokens_por_fator
        )
        
        resultados_consolidados = []
        
        print(f"Iniciando processamento de {len(atividades_financeiras)} atividades principais...")
        atividades_financeiras = [atividades_financeiras[0]]
        for atividade_financeira in atividades_financeiras:
            print(f"\nProcessando atividade: {atividade_financeira}")
            resultado_df, resultado_texto, file_path = processor.process_activity(atividade_financeira)
            
            if not resultado_df.empty and "Erro" not in resultado_df.columns:
                resultados_consolidados.append(resultado_df)          
            
            print(f"\nAtividade Analisada: {atividade_financeira}")
            print(f"Arquivo salvo em: {file_path}")
            print("\nResultado (DataFrame):")
            print(resultado_df)
            
        if resultados_consolidados:
            df_consolidado = pd.concat(resultados_consolidados)
            df_consolidado["Atividade"] = df_consolidado["Atividade"].map(processor.atividade_mapping).fillna(df_consolidado["Atividade"])
            consolidado_path = os.path.join("resultados", "resultados_consolidados.xlsx")
            df_consolidado.to_excel(consolidado_path, index=False, engine="openpyxl")
            print(f"\nResultados consolidados salvos em: {consolidado_path}")
            
    except Exception as e:
        print(f"Erro: {str(e)}")
        print("\nInformações do ambiente:")
        print(f"Python version: {os.sys.version}")
        print("Variáveis de ambiente necessárias:")
        print(f"- XAI_API_KEY definida: {'Sim' if os.getenv('XAI_API_KEY') else 'Não'}")

if __name__ == "__main__":
    main()