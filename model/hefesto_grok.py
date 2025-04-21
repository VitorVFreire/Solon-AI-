import pandas as pd
import requests
import sqlite3
import os
from typing import List, Dict, Any, Tuple
from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.documents import Document
from langgraph.graph import END, StateGraph

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
            # Aumentando max_tokens para acomodar respostas mais detalhadas
            "max_tokens": 1500,
            # Reduzindo temperature para obter resultados mais consistentes
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

# Classe para gerenciar o RAG com as atividades
class AtividadesRAG:
    def __init__(self, atividades_data: List[Dict[str, Any]], max_context=15):
        # Aumentando o tamanho máximo do contexto para incluir mais atividades
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
            # Adicionando mais metadados para melhorar a recuperação de contexto
            content = f"{numero}:{descricao}"
            metadata = {
                "numero": numero, 
                "descricao": descricao,
                # Adicionando categoria e subcategoria se disponíveis
                "categoria": atividade.get('categoria', ''),
                "subcategoria": atividade.get('subcategoria', '')
            }
            documents.append(Document(page_content=content, metadata=metadata))
        
        # Melhorando a busca por similaridade
        class EnhancedTextSimilarity:
            def __init__(self, documents):
                self.documents = documents
                
            def similarity_search(self, query, k=5):
                def enhanced_similarity(doc, query):
                    # Tokenização mais robusta
                    doc_words = set(re.sub(r'[^\w\s]', ' ', doc.page_content.lower()).split())
                    query_words = set(re.sub(r'[^\w\s]', ' ', query.lower()).split())
                    
                    # Adiciona pontuação extra para correspondências de categorias
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
    def __init__(self, rag_system: AtividadesRAG, llm_client, output_dir: str = "resultados"):
        self.rag_system = rag_system
        self.llm_client = llm_client
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.build_graph()
    
    def retrieve(self, state: Dict[str, Any]) -> Dict[str, Any]:
        query = state["atividade_financeira"]
        
        # Melhorando a seleção de atividades para o contexto
        # Priorizando diversidade de setores
        context_activities = []
        sectors_included = set()
        
        for atividade in self.rag_system.atividades_data:
            numero = atividade.get('numero') or atividade.get('codigo_atividade')
            descricao = atividade.get('descricao') or atividade.get('nome_atividade')
            categoria = atividade.get('categoria', '')
            
            if descricao != query:
                # Verificar se já temos muitas atividades deste setor
                if len([a for a in context_activities if categoria in a]) < 3 or len(sectors_included) < 5:
                    sectors_included.add(categoria)
                    # Incluindo mais detalhes no contexto para melhorar a análise
                    atividade_str = f"{numero}:{descricao}"
                    if categoria:
                        atividade_str += f" [Setor: {categoria}]"
                    context_activities.append(atividade_str)
        
        # Ordenando para garantir diversidade e representatividade
        max_context = min(len(context_activities), self.rag_system.max_context)
        context = context_activities[:max_context]
        state["context"] = "\n".join(context)
        return state
    
    def generate_dependencies(self, state: Dict[str, Any]) -> Dict[str, Any]:
        # Usando o novo prompt aprimorado
        system_prompt = """
        Você é um analista especializado em modelagem de dependências intersectoriais econômicas com experiência em análise de redes complexas e propagação de impactos econômicos.

        TAREFA:
        Analise a atividade econômica principal fornecida e determine o grau de dependência que cada atividade listada no contexto tem em relação a esta atividade principal, utilizando uma escala precisa de 0 a 5:

        - 0: Nenhuma dependência (setores economicamente independentes)
        - 1: Dependência muito baixa (interrupções na atividade principal causam efeitos mínimos e facilmente contornáveis)
        - 2: Dependência baixa (impactos limitados e de curto prazo)
        - 3: Dependência moderada (impactos significativos que exigem adaptação)
        - 4: Dependência alta (interrupções na atividade principal causam disfunções operacionais graves)
        - 5: Dependência muito alta (dependência crítica; inviabilidade operacional sem a atividade principal)

        FATORES DE ANÁLISE:
        Para cada setor, avalie quantitativamente:

        1. Integração da cadeia produtiva:
        - Percentual de insumos diretos provenientes da atividade principal
        - Impossibilidade de substituição por fornecedores alternativos
        - Posição na cadeia (upstream/downstream)

        2. Interdependência financeira:
        - Fluxos financeiros críticos entre os setores
        - Impacto de variações de preço da atividade principal
        - Correlação histórica de desempenho financeiro

        3. Dependências estruturais:
        - Infraestrutura física compartilhada
        - Sistemas tecnológicos integrados
        - Dependência de conhecimento especializado comum

        4. Vulnerabilidade regulatória:
        - Exposição a regulamentações compartilhadas
        - Impacto de alterações regulatórias no setor principal

        5. Dependência de mercado:
        - Base de clientes compartilhada
        - Canais de distribuição comuns
        - Impacto na demanda derivada

        6. Fator de propagação de impacto:
        - Velocidade de transmissão de choques econômicos
        - Capacidade de isolamento contra falhas
        - Efeito multiplicador na economia

        METODOLOGIA DE PONTUAÇÃO:
        - A classificação final deve ser derivada de um cálculo ponderado dos fatores acima
        - Para cada fator, atribua um valor de 0-5 e depois calcule a média ponderada
        - Considere efeitos de retroalimentação (feedback loops) entre setores

        RESULTADO:
        Forneça o resultado como uma tabela estruturada com as seguintes colunas:
        | Setor Econômico | Grau de Dependência | Coeficiente de Propagação | Justificativa Detalhada |
        |-----------------|---------------------|---------------------------|-------------------------|

        O Coeficiente de Propagação (0.0-1.0) indica quanto do impacto no setor principal será transmitido para este setor, considerando:
        - Proximidade na rede de dependências
        - Resiliência estrutural do setor
        - Capacidade de adaptação a choques

        MODELAGEM DE PROPAGAÇÃO:
        Inclua uma seção final explicando como os impactos se propagariam através da rede de setores conectados, incluindo:
        - Setores que funcionam como hubs de transmissão
        - Possíveis efeitos em cascata
        - Pontos de vulnerabilidade crítica na rede
        - Estimativa do tempo de propagação dos impactos

        Considere todos os setores no contexto e inclua apenas aqueles com algum grau de dependência mensurável (1-5).
        """
        
        human_prompt = """
        Atividade Principal para análise: {atividade_financeira}
        
        Lista de Atividades do Contexto:
        {context}
        
        Determine o grau de dependência (0-5) e o coeficiente de propagação (0.0-1.0) que cada atividade do contexto tem em relação à atividade principal "{atividade_financeira}".
        Forneça uma justificativa detalhada para cada grau atribuído, considerando todos os fatores mencionados.
        
        Inclua na sua resposta:
        1. A tabela completa com Setor Econômico, Grau de Dependência, Coeficiente de Propagação e Justificativa Detalhada
        2. A seção de Modelagem de Propagação explicando os possíveis efeitos em cascata e vulnerabilidades
        
        Inclua apenas atividades com grau de dependência maior que 0.
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
            # Extraindo a tabela e a seção de modelagem
            table_section = ""
            modelagem_section = ""
            
            # Separar a tabela e a seção de modelagem
            if "MODELAGEM DE PROPAGAÇÃO" in result_text:
                parts = result_text.split("MODELAGEM DE PROPAGAÇÃO", 1)
                table_section = parts[0].strip()
                modelagem_section = "MODELAGEM DE PROPAGAÇÃO" + parts[1].strip()
            else:
                table_section = result_text
            
            # Processar a tabela
            lines = table_section.strip().split('\n')
            data = []
            
            for line in lines:
                if '|' in line:
                    parts = [p.strip() for p in line.split('|') if p.strip()]
                    if len(parts) >= 4 and not all(c in '-:|' for c in parts[0]):
                        try:
                            # Extrair apenas o nome da atividade (sem número)
                            atividade = parts[0].split(':')[-1].strip() if ':' in parts[0] else parts[0].strip()
                            dependencia = int(parts[1].strip())
                            
                            # Extrair o coeficiente de propagação
                            propagacao = float(parts[2].strip())
                            
                            # Garantir que o coeficiente está no intervalo [0,1]
                            propagacao = max(0.0, min(1.0, propagacao))
                            
                            justificativa = parts[3].strip()
                            
                            if 0 < dependencia <= 5:
                                data.append({
                                    "Atividade": atividade,
                                    "Grau de Dependência": dependencia,
                                    "Coeficiente de Propagação": propagacao,
                                    "Justificativa": justificativa,
                                    "Atividade Original": state["atividade_financeira"]
                                })
                        except (ValueError, IndexError) as e:
                            print(f"Erro ao processar linha: {line} - {str(e)}")
                            continue
            
            result_df = pd.DataFrame(data)
            
            # Adicionar a seção de modelagem como metadados
            if not result_df.empty:
                state["formatted_result"] = result_df
                state["modelagem_propagacao"] = modelagem_section
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
        
        # Criar um writer para o arquivo Excel
        with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
            # Reorganizar colunas
            if not df.empty and "Erro" not in df.columns:
                columns = ["Atividade", "Grau de Dependência", "Coeficiente de Propagação", "Justificativa", "Atividade Original"]
                df = df[columns]
            
            # Salvar a tabela principal na primeira planilha
            df.to_excel(writer, sheet_name="Dependências", index=False)
            
            # Adicionar a seção de modelagem em uma segunda planilha
            if modelagem_propagacao:
                # Criar um DataFrame com a seção de modelagem
                modelagem_df = pd.DataFrame({"Modelagem de Propagação": [modelagem_propagacao]})
                modelagem_df.to_excel(writer, sheet_name="Modelagem de Propagação", index=False)
        
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

# Função para gerar um grafo de rede baseado nos resultados
def gerar_grafo_de_rede(resultados_df: pd.DataFrame, threshold_dependencia: int = 2):
    """
    Gera um grafo de rede visualizável baseado nos resultados das dependências
    
    Args:
        resultados_df: DataFrame com os resultados de dependência
        threshold_dependencia: Limiar mínimo de dependência para incluir no grafo
    
    Returns:
        Um grafo NetworkX e o caminho para a visualização salva
    """
    import networkx as nx
    import matplotlib.pyplot as plt
    from matplotlib.colors import LinearSegmentedColormap
    
    # Filtrar apenas dependências significativas
    df_filtrado = resultados_df[resultados_df['Grau de Dependência'] >= threshold_dependencia]
    
    if df_filtrado.empty:
        print("Nenhuma dependência significativa encontrada para gerar o grafo.")
        return None, None
    
    # Criar grafo direcionado
    G = nx.DiGraph()
    
    # Adicionar nós e arestas
    atividades_principais = set(df_filtrado['Atividade Original'])
    atividades_dependentes = set(df_filtrado['Atividade'])
    
    # Adicionar todos os nós
    all_nodes = atividades_principais.union(atividades_dependentes)
    for node in all_nodes:
        G.add_node(node)
    
    # Adicionar arestas com atributos
    for _, row in df_filtrado.iterrows():
        origem = row['Atividade Original']
        destino = row['Atividade']
        dependencia = row['Grau de Dependência']
        propagacao = row['Coeficiente de Propagação']
        
        G.add_edge(
            origem, 
            destino, 
            weight=dependencia,
            propagacao=propagacao,
            justificativa=row['Justificativa']
        )
    
    # Calcular centralidade para dimensionar os nós
    centrality = nx.betweenness_centrality(G)
    
    # Definir cores para as arestas baseadas no grau de dependência
    colors = ["yellow", "orange", "red", "darkred"]
    cmap = LinearSegmentedColormap.from_list("dependencia_cmap", colors)
    
    # Preparar para a visualização
    plt.figure(figsize=(16, 12))
    
    # Posicionar os nós
    pos = nx.spring_layout(G, k=0.5, iterations=50)
    
    # Desenhar nós
    nx.draw_networkx_nodes(
        G, 
        pos, 
        node_size=[v * 5000 + 500 for v in centrality.values()],
        node_color="skyblue",
        alpha=0.8
    )
    
    # Desenhar arestas com cores baseadas no grau de dependência
    edges = G.edges(data=True)
    weights = [data['weight'] for _, _, data in edges]
    normalized_weights = [(w - min(weights)) / (max(weights) - min(weights) + 0.01) for w in weights]
    
    nx.draw_networkx_edges(
        G, 
        pos, 
        width=2,
        edge_color=normalized_weights,
        edge_cmap=cmap,
        alpha=0.7,
        connectionstyle="arc3,rad=0.1"
    )
    
    # Adicionar rótulos aos nós
    nx.draw_networkx_labels(
        G, 
        pos, 
        font_size=10,
        font_family="sans-serif"
    )
    
    # Adicionar um título
    plt.title("Grafo de Dependências Econômicas Intersectoriais", fontsize=16)
    plt.axis("off")
    
    # Adicionar legenda de cores
    sm = plt.cm.ScalarMappable(cmap=cmap)
    sm.set_array([])
    cbar = plt.colorbar(sm, ax=plt.gca())
    cbar.set_label('Grau de Dependência')
    
    # Salvar a visualização
    output_path = "grafo_dependencias.png"
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    
    print(f"Grafo de rede salvo em: {output_path}")
    return G, output_path

# Função para calcular o impacto em cascata
def calcular_impacto_em_cascata(G, node_inicial, impacto_inicial=1.0, max_depth=5):
    """
    Calcula o impacto em cascata a partir de um nó inicial
    
    Args:
        G: Grafo NetworkX com as dependências
        node_inicial: Nó onde o impacto se origina
        impacto_inicial: Valor do impacto inicial (1.0 = 100%)
        max_depth: Profundidade máxima de propagação
    
    Returns:
        DataFrame com os impactos calculados em cada nó
    """
    impactos = {node: 0.0 for node in G.nodes()}
    impactos[node_inicial] = impacto_inicial
    
    # Fila para BFS: (nó, profundidade, impacto)
    queue = [(node_inicial, 0, impacto_inicial)]
    visitados = set()
    
    while queue:
        node, depth, impacto = queue.pop(0)
        
        if depth >= max_depth:
            continue
        
        if (node, depth) in visitados:
            continue
            
        visitados.add((node, depth))
        
        # Propagar para vizinhos
        for vizinho in G.neighbors(node):
            edge_data = G.get_edge_data(node, vizinho)
            grau_dependencia = edge_data['weight']
            coef_propagacao = edge_data['propagacao']
            
            # Calcular impacto propagado
            impacto_propagado = impacto * (grau_dependencia / 5.0) * coef_propagacao
            
            # Acumular impacto no vizinho
            impactos[vizinho] += impacto_propagado
            
            # Adicionar vizinho à fila
            queue.append((vizinho, depth + 1, impacto_propagado))
    
    # Converter para DataFrame
    impacto_df = pd.DataFrame({
        'Setor': list(impactos.keys()),
        'Impacto Calculado': list(impactos.values())
    })
    
    # Ordenar por impacto
    impacto_df = impacto_df.sort_values('Impacto Calculado', ascending=False)
    
    return impacto_df

# Código de teste
def main_test():
    atividades_exemplo = [
        {"numero": "01.11-3", "descricao": "Cultivo de cereais"},
        {"numero": "01.12-1", "descricao": "Cultivo de algodão herbáceo e de outras fibras de lavoura temporária"},
        {"numero": "01.13-0", "descricao": "Cultivo de cana-de-açúcar"},
        {"numero": "01.14-8", "descricao": "Cultivo de fumo"},
        {"numero": "01.15-6", "descricao": "Cultivo de soja"},
        {"numero": "01.33-4", "descricao": "Cultivo de frutas de lavoura permanente, exceto laranja e uva"},
        {"numero": "01.34-2", "descricao": "Cultivo de café"},
        {"numero": "01.35-1", "descricao": "Cultivo de cacau"},
        {"numero": "64.33-6", "descricao": "Bancos de desenvolvimento"},
        {"numero": "64.34-4", "descricao": "Agências de fomento"},
        {"numero": "64.35-2", "descricao": "Crédito imobiliário"},
        {"numero": "64.36-1", "descricao": "Sociedades de crédito, financiamento e investimento - financeiras"},
        {"numero": "64.37-9", "descricao": "Sociedades de crédito ao microempreendedor"}
    ]
    
    atividade_financeira = "Telecomunicações por fio"
    
    try:
        xai_config = setup_xai_client()
        xai_client = XAIClient(xai_config)
        
        print("Configurando sistema RAG...")
        rag_system = AtividadesRAG(atividades_exemplo, max_context=30)
        
        print("Configurando processador de atividades...")
        processor = AtividadesProcessor(rag_system, xai_client, output_dir="resultados_teste")
        
        print(f"\nProcessando atividade: {atividade_financeira}")
        resultado_df, resultado_texto, file_path = processor.process_activity(atividade_financeira)
        
        print(f"\nAtividade Analisada: {atividade_financeira}")
        print(f"Arquivo salvo em: {file_path}")
        print("\nResultado (DataFrame):")
        print(resultado_df)
        print("\nResultado (Texto):")
        print(resultado_texto)
        
        '''
        # Descomentar para testar múltiplas atividades
        atividades_financeiras = [
            "Telecomunicações por fio",
            "Geração de energia elétrica",
            "Bancos comerciais",
            "Extração de petróleo e gás natural",
            "Cultivo de soja"
        ]
        
        resultados_consolidados = []
        
        for atividade_financeira in atividades_financeiras:
            print(f"\nProcessando atividade: {atividade_financeira}")
            resultado_df, resultado_texto, file_path = processor.process_activity(atividade_financeira)
            
            if not resultado_df.empty and "Erro" not in resultado_df.columns:
                resultados_consolidados.append(resultado_df)
            
            print(f"\nAtividade Analisada: {atividade_financeira}")
            print(f"Arquivo salvo em: {file_path}")
        
        if resultados_consolidados:
            df_consolidado = pd.concat(resultados_consolidados)
            consolidado_path = os.path.join("resultados_teste", "resultados_consolidados.xlsx")
            df_consolidado.to_excel(consolidado_path, index=False, engine="openpyxl")
            print(f"\nResultados consolidados salvos em: {consolidado_path}")
        '''
            
    except Exception as e:
        print(f"Erro: {str(e)}")
        print("\nInformações do ambiente:")
        print(f"Python version: {os.sys.version}")
        print("Variáveis de ambiente necessárias:")
        print(f"- XAI_API_KEY definida: {'Sim' if os.getenv('XAI_API_KEY') else 'Não'}")

# Código para produção
def main():
    try:
        conn = sqlite3.connect("../data_scraper/cnpj.db")
        query = "SELECT codigo_atividade, nome_atividade FROM cnaes"
        df_atividades = pd.read_sql_query(query, conn)
        conn.close()
        
        atividades_exemplo = df_atividades.rename(columns={
            "codigo_atividade": "numero",
            "nome_atividade": "descricao"
        }).to_dict(orient="records")
        
        max_atividades = 100
        atividades_exemplo = atividades_exemplo[:max_atividades]
        
        df_financeiras = pd.read_excel("atividades_analisadas/para_analise.xlsx")
        atividades_financeiras = df_financeiras["Atividades"].tolist()
        
        print(len(atividades_exemplo))

        xai_config = setup_xai_client()
        xai_client = XAIClient(xai_config)
        
        print(f"Configurando sistema RAG com {len(atividades_exemplo)} atividades...")
        rag_system = AtividadesRAG(atividades_exemplo, max_context=30)
        
        print("Configurando processador de atividades...")
        processor = AtividadesProcessor(rag_system, xai_client, output_dir="resultados")
        
        resultados_consolidados = []
        
        print(f"Iniciando processamento de {len(atividades_financeiras)} atividades principais...")
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
    main_test()  # Executar versão de teste
    # main()  # Versão para produção