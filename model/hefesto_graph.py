"""
Neo4j Integration for Economic Sector Dependencies

Este script cria um grafo de dependências econômicas no Neo4j a partir dos
resultados gerados pelo programa de análise de dependências intersectoriais.

Requisitos:
- Neo4j instalado e rodando
- Python 3.7+
- pandas
- neo4j (driver para Python)
"""

import os
import pandas as pd
import glob
from neo4j import GraphDatabase
from typing import List, Dict, Any, Optional


class Neo4jDependencyGraph:
    """
    Classe para criar e gerenciar um grafo de dependências econômicas no Neo4j
    """
    
    def __init__(self, uri: str, username: str, password: str, database: str = "neo4j"):
        """
        Inicializa a conexão com o banco de dados Neo4j
        
        Args:
            uri: URI do servidor Neo4j (exemplo: "neo4j://localhost:7687")
            username: Nome de usuário para autenticação
            password: Senha para autenticação
            database: Nome do banco de dados a ser utilizado (padrão: "neo4j")
        """
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
        self.database = database
        print(f"Conectado ao Neo4j em {uri}")
    
    def close(self):
        """Fecha a conexão com o banco de dados"""
        if self.driver:
            self.driver.close()
    
    def clear_database(self):
        """Limpa todos os nós e relacionamentos do banco de dados"""
        with self.driver.session(database=self.database) as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("Banco de dados limpo com sucesso")
    
    def create_schema(self):
        """Cria o esquema do grafo com índices e constraints"""
        with self.driver.session(database=self.database) as session:
            # Criar constraint para garantir unicidade de setores econômicos
            session.run("""
                CREATE CONSTRAINT setor_economico_nome IF NOT EXISTS 
                FOR (s:SetorEconomico) REQUIRE s.nome IS UNIQUE
            """)
            
            # Criar índices para melhorar o desempenho das consultas
            session.run("CREATE INDEX setor_categoria IF NOT EXISTS FOR (s:SetorEconomico) ON (s.categoria)")
            session.run("CREATE INDEX dependencia_grau IF NOT EXISTS FOR ()-[r:DEPENDE_DE]-() ON (r.grau)")
            session.run("CREATE INDEX dependencia_coef_propagacao IF NOT EXISTS FOR ()-[r:DEPENDE_DE]-() ON (r.coef_propagacao)")
            
            print("Esquema criado com sucesso")
    
    def create_sector_node(self, nome: str, categoria: Optional[str] = None):
        """
        Cria um nó de setor econômico no grafo
        
        Args:
            nome: Nome do setor econômico
            categoria: Categoria do setor (opcional)
        """
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MERGE (s:SetorEconomico {nome: $nome})
                ON CREATE SET 
                    s.categoria = $categoria,
                    s.criado_em = timestamp()
                ON MATCH SET
                    s.atualizado_em = timestamp()
                RETURN s
            """, nome=nome, categoria=categoria)
            
            record = result.single()
            if record:
                print(f"Setor criado/atualizado: {nome}")
            return record
    
    def create_dependency_relationship(self, setor_origem: str, setor_destino: str, 
                                       grau: int, coef_propagacao: float, justificativa: str):
        """
        Cria um relacionamento de dependência entre dois setores
        
        Args:
            setor_origem: Nome do setor que depende
            setor_destino: Nome do setor do qual se depende
            grau: Grau de dependência (0-5)
            coef_propagacao: Coeficiente de propagação (0.0-1.0)
            justificativa: Justificativa para o grau de dependência
        """
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH (origem:SetorEconomico {nome: $setor_origem})
                MATCH (destino:SetorEconomico {nome: $setor_destino})
                MERGE (origem)-[r:DEPENDE_DE]->(destino)
                ON CREATE SET 
                    r.grau = $grau,
                    r.coef_propagacao = $coef_propagacao,
                    r.justificativa = $justificativa,
                    r.criado_em = timestamp()
                ON MATCH SET
                    r.grau = $grau,
                    r.coef_propagacao = $coef_propagacao,
                    r.justificativa = $justificativa,
                    r.atualizado_em = timestamp()
                RETURN r
            """, setor_origem=setor_origem, setor_destino=setor_destino, 
                  grau=grau, coef_propagacao=coef_propagacao, justificativa=justificativa)
            
            record = result.single()
            if record:
                print(f"Dependência criada/atualizada: {setor_origem} -> {setor_destino} (Grau: {grau})")
            return record
    
    def import_excel_results(self, excel_path: str):
        """
        Importa os resultados de um arquivo Excel de dependências para o Neo4j
        
        Args:
            excel_path: Caminho para o arquivo Excel com os resultados
        """
        try:
            # Carregar o arquivo Excel
            df = pd.read_excel(excel_path)
            
            if df.empty:
                print(f"Arquivo vazio: {excel_path}")
                return 0
            
            if "Erro" in df.columns:
                print(f"Arquivo contém erros: {excel_path}")
                return 0
            
            # Verificar se temos as colunas necessárias
            required_columns = ["Atividade", "Grau de Dependência", "Coeficiente de Propagação", 
                               "Justificativa", "Atividade Original"]
            
            if not all(col in df.columns for col in required_columns):
                print(f"Colunas necessárias não encontradas em: {excel_path}")
                return 0
            
            # Obter a atividade principal
            atividade_principal = df["Atividade Original"].iloc[0]
            
            # Criar nó para a atividade principal (sem categoria)
            self.create_sector_node(atividade_principal)
            
            # Processar cada linha de dependência
            count = 0
            for _, row in df.iterrows():
                atividade_dependente = row["Atividade"]
                grau_dependencia = int(row["Grau de Dependência"])
                coef_propagacao = float(row["Coeficiente de Propagação"])
                justificativa = row["Justificativa"]
                
                # Criar nó para a atividade dependente (sem categoria)
                self.create_sector_node(atividade_dependente)
                
                # Criar relacionamento de dependência
                self.create_dependency_relationship(
                    atividade_dependente, 
                    atividade_principal, 
                    grau_dependencia, 
                    coef_propagacao, 
                    justificativa
                )
                count += 1
            
            print(f"Importado {count} dependências de {excel_path}")
            return count
            
        except Exception as e:
            print(f"Erro ao importar {excel_path}: {str(e)}")
            return 0
    
    def import_all_results(self, results_dir: str):
        """
        Importa todos os arquivos Excel de resultados de um diretório
        
        Args:
            results_dir: Diretório contendo os arquivos Excel de resultados
        """
        excel_files = glob.glob(os.path.join(results_dir, "resultado_*.xlsx"))
        
        if not excel_files:
            print(f"Nenhum arquivo de resultado encontrado em {results_dir}")
            return 0
        
        total_imported = 0
        for excel_file in excel_files:
            count = self.import_excel_results(excel_file)
            total_imported += count
        
        print(f"Total de {total_imported} dependências importadas de {len(excel_files)} arquivos")
        return total_imported
    
    def import_impact_results(self, impact_dir: str):
        """
        Importa os resultados de impacto em cascata e adiciona como propriedades dos nós
        
        Args:
            impact_dir: Diretório contendo os arquivos Excel de impacto em cascata
        """
        impact_files = glob.glob(os.path.join(impact_dir, "impacto_cascata_*.xlsx"))
        
        if not impact_files:
            print(f"Nenhum arquivo de impacto encontrado em {impact_dir}")
            return 0
        
        total_sectors_updated = 0
        
        for impact_file in impact_files:
            try:
                df = pd.read_excel(impact_file)
                
                # Extrair o nome da atividade original do nome do arquivo
                filename = os.path.basename(impact_file)
                atividade_original = filename.replace("impacto_cascata_", "").replace(".xlsx", "")
                atividade_original = atividade_original.replace("_", " ")
                
                # Verificar se temos as colunas necessárias
                if "Setor" not in df.columns or "Impacto Calculado" not in df.columns:
                    print(f"Colunas necessárias não encontradas em: {impact_file}")
                    continue
                
                # Atualizar cada setor com o impacto calculado
                count = 0
                for _, row in df.iterrows():
                    setor = row["Setor"]
                    impacto = float(row["Impacto Calculado"])
                    
                    # Se o impacto for muito pequeno, pular
                    if impacto < 0.01:
                        continue
                    
                    # Atualizar o nó do setor com o impacto
                    with self.driver.session(database=self.database) as session:
                        result = session.run("""
                            MATCH (s:SetorEconomico {nome: $setor})
                            SET s.impacto_de_$origem = $impacto
                            RETURN s
                        """, setor=setor, origem=atividade_original.replace(" ", "_"), impacto=impacto)
                        
                        if result.single():
                            count += 1
                
                print(f"Atualizados {count} setores com impacto de {atividade_original}")
                total_sectors_updated += count
                
            except Exception as e:
                print(f"Erro ao importar impactos de {impact_file}: {str(e)}")
        
        return total_sectors_updated
    
    def run_analytics_queries(self):
        """
        Executa consultas analíticas no grafo e retorna os resultados
        """
        analytics_results = {}
        
        with self.driver.session(database=self.database) as session:
            # 1. Setores mais dependentes (maior número de dependências)
            most_dependent = session.run("""
                MATCH (s:SetorEconomico)-[r:DEPENDE_DE]->()
                RETURN s.nome AS setor, count(r) AS num_dependencias
                ORDER BY num_dependencias DESC
                LIMIT 10
            """)
            
            analytics_results["setores_mais_dependentes"] = [
                {"setor": record["setor"], "num_dependencias": record["num_dependencias"]}
                for record in most_dependent
            ]
            
            # 2. Setores mais influentes (mais setores dependem deles)
            most_influential = session.run("""
                MATCH (s:SetorEconomico)<-[r:DEPENDE_DE]-()
                RETURN s.nome AS setor, count(r) AS num_dependentes, 
                       sum(r.grau) AS soma_dependencia,
                       avg(r.coef_propagacao) AS media_propagacao
                ORDER BY soma_dependencia DESC
                LIMIT 10
            """)
            
            analytics_results["setores_mais_influentes"] = [
                {
                    "setor": record["setor"], 
                    "num_dependentes": record["num_dependentes"],
                    "soma_dependencia": record["soma_dependencia"],
                    "media_propagacao": record["media_propagacao"]
                }
                for record in most_influential
            ]
            
            # 3. Dependências mais fortes (maior grau)
            strongest_dependencies = session.run("""
                MATCH (origem:SetorEconomico)-[r:DEPENDE_DE]->(destino:SetorEconomico)
                WHERE r.grau >= 4
                RETURN origem.nome AS setor_dependente, 
                       destino.nome AS setor_influenciador,
                       r.grau AS grau_dependencia,
                       r.coef_propagacao AS coef_propagacao
                ORDER BY r.grau DESC, r.coef_propagacao DESC
                LIMIT 20
            """)
            
            analytics_results["dependencias_mais_fortes"] = [
                {
                    "setor_dependente": record["setor_dependente"],
                    "setor_influenciador": record["setor_influenciador"],
                    "grau_dependencia": record["grau_dependencia"],
                    "coef_propagacao": record["coef_propagacao"]
                }
                for record in strongest_dependencies
            ]
            
            # 4. Caminhos críticos de propagação (caminhos mais longos com alto coeficiente)
            critical_paths = session.run("""
                MATCH path = (s1:SetorEconomico)-[r1:DEPENDE_DE*2..5]->(s2:SetorEconomico)
                WHERE all(r in relationships(path) WHERE r.grau >= 3 AND r.coef_propagacao >= 0.6)
                RETURN [node in nodes(path) | node.nome] AS caminho,
                       reduce(total = 1.0, r in relationships(path) | total * r.coef_propagacao) AS propagacao_total,
                       length(path) AS tamanho_caminho
                ORDER BY propagacao_total DESC, tamanho_caminho DESC
                LIMIT 10
            """)
            
            analytics_results["caminhos_criticos"] = [
                {
                    "caminho": record["caminho"],
                    "propagacao_total": record["propagacao_total"],
                    "tamanho_caminho": record["tamanho_caminho"]
                }
                for record in critical_paths
            ]
            
            # 5. Clusters de setores interdependentes
            sector_clusters = session.run("""
                CALL gds.graph.project(
                  'sector_dependencies',
                  'SetorEconomico',
                  {
                    DEPENDE_DE: {
                      orientation: 'UNDIRECTED',
                      properties: ['grau', 'coef_propagacao']
                    }
                  }
                )
                YIELD graphName
                
                CALL gds.louvain.stream('sector_dependencies')
                YIELD nodeId, communityId
                
                WITH gds.util.asNode(nodeId) AS setor, communityId
                
                RETURN communityId AS cluster_id, 
                       collect(setor.nome) AS setores,
                       count(*) AS tamanho_cluster
                ORDER BY tamanho_cluster DESC
                LIMIT 10
            """, timeout=60000)  # Aumentar timeout para consultas mais pesadas
            
            try:
                analytics_results["clusters_setores"] = [
                    {
                        "cluster_id": record["cluster_id"],
                        "setores": record["setores"],
                        "tamanho_cluster": record["tamanho_cluster"]
                    }
                    for record in sector_clusters
                ]
            except Exception as e:
                # Se o algoritmo Louvain falhar (por exemplo, se o GDS não estiver instalado)
                print(f"Aviso: Não foi possível executar o algoritmo de clusterização: {str(e)}")
                analytics_results["clusters_setores"] = []
        
        return analytics_results
    
    def export_analytics_to_excel(self, analytics_results: Dict[str, List[Dict]], output_path: str):
        """
        Exporta os resultados das análises para um arquivo Excel
        
        Args:
            analytics_results: Resultados das análises
            output_path: Caminho para salvar o arquivo Excel
        """
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            # Setores mais dependentes
            if "setores_mais_dependentes" in analytics_results:
                df_dependentes = pd.DataFrame(analytics_results["setores_mais_dependentes"])
                df_dependentes.to_excel(writer, sheet_name="Setores Mais Dependentes", index=False)
            
            # Setores mais influentes
            if "setores_mais_influentes" in analytics_results:
                df_influentes = pd.DataFrame(analytics_results["setores_mais_influentes"])
                df_influentes.to_excel(writer, sheet_name="Setores Mais Influentes", index=False)
            
            # Dependências mais fortes
            if "dependencias_mais_fortes" in analytics_results:
                df_deps_fortes = pd.DataFrame(analytics_results["dependencias_mais_fortes"])
                df_deps_fortes.to_excel(writer, sheet_name="Dependências Fortes", index=False)
            
            # Caminhos críticos
            if "caminhos_criticos" in analytics_results:
                df_caminhos = pd.DataFrame(analytics_results["caminhos_criticos"])
                # Converter a lista de caminhos para string para salvar corretamente
                df_caminhos["caminho"] = df_caminhos["caminho"].apply(lambda x: " -> ".join(x))
                df_caminhos.to_excel(writer, sheet_name="Caminhos Críticos", index=False)
            
            # Clusters de setores
            if "clusters_setores" in analytics_results and analytics_results["clusters_setores"]:
                df_clusters = pd.DataFrame(analytics_results["clusters_setores"])
                # Converter a lista de setores para string para salvar corretamente
                df_clusters["setores"] = df_clusters["setores"].apply(lambda x: ", ".join(x))
                df_clusters.to_excel(writer, sheet_name="Clusters", index=False)
        
        print(f"Análises exportadas para: {output_path}")


def create_cypher_queries_file(output_path: str):
    """
    Cria um arquivo com consultas Cypher úteis para analisar o grafo
    
    Args:
        output_path: Caminho para salvar o arquivo
    """
    queries = [
        {
            "nome": "1. Todas as dependências de um setor",
            "descricao": "Encontra todos os setores que dependem de um setor específico",
            "query": """
                MATCH (s:SetorEconomico)-[r:DEPENDE_DE]->(alvo:SetorEconomico {nome: "SETOR_AQUI"})
                RETURN s.nome AS setor_dependente, r.grau AS grau_dependencia, 
                       r.coef_propagacao AS coef_propagacao, r.justificativa
                ORDER BY r.grau DESC
            """
        },
        {
            "nome": "2. Setores que mais sofreriam impacto de um colapso",
            "descricao": "Calcula o impacto em cascata a partir de um setor específico",
            "query": """
                MATCH (origem:SetorEconomico {nome: "SETOR_AQUI"})
                CALL apoc.path.expandConfig(origem, {
                    relationshipFilter: "<DEPENDE_DE",
                    minLevel: 1,
                    maxLevel: 5
                })
                YIELD path
                WITH nodes(path) AS nos, relationships(path) AS relacoes,
                     reduce(total = 1.0, r in relationships(path) | total * r.coef_propagacao) AS propagacao
                UNWIND nos AS no
                WHERE no <> origem
                RETURN no.nome AS setor_impactado, 
                       max(propagacao) AS impacto_propagado
                ORDER BY impacto_propagado DESC
                LIMIT 20
            """
        },
        {
            "nome": "3. Vulnerabilidades estruturais da rede",
            "descricao": "Identifica setores que são pontos críticos na rede de dependências",
            "query": """
                CALL gds.betweenness.stream('sector_dependencies')
                YIELD nodeId, score
                RETURN gds.util.asNode(nodeId).nome AS setor,
                       score AS centralidade_intermediacao
                ORDER BY score DESC
                LIMIT 10
            """
        },
        {
            "nome": "4. Comunidades de setores interdependentes",
            "descricao": "Identifica clusters de setores que são altamente interdependentes",
            "query": """
                CALL gds.louvain.stream('sector_dependencies')
                YIELD nodeId, communityId
                RETURN gds.util.asNode(nodeId).nome AS setor,
                       communityId AS cluster
                ORDER BY communityId ASC, setor ASC
            """
        },
        {
            "nome": "5. Riscos sistêmicos",
            "descricao": "Identifica setores que poderiam causar colapsos em cascata",
            "query": """
                MATCH (s:SetorEconomico)
                OPTIONAL MATCH (s)<-[r:DEPENDE_DE]-(dependente)
                WITH s, count(r) AS num_dependentes, 
                     sum(r.grau) AS soma_dependencia,
                     avg(r.coef_propagacao) AS media_propagacao
                WHERE num_dependentes > 5 AND media_propagacao > 0.7
                RETURN s.nome AS setor_risco_sistemico,
                       num_dependentes,
                       soma_dependencia,
                       media_propagacao,
                       (soma_dependencia * media_propagacao) AS indice_risco
                ORDER BY indice_risco DESC
                LIMIT 10
            """
        }
    ]
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("# Consultas Cypher para Análise do Grafo de Dependências Econômicas\n\n")
        
        for query_info in queries:
            f.write(f"## {query_info['nome']}\n\n")
            f.write(f"{query_info['descricao']}\n\n")
            f.write("```cypher\n")
            f.write(query_info['query'].strip())
            f.write("\n```\n\n")
    
    print(f"Arquivo de consultas Cypher criado em: {output_path}")


def main():
    """Função principal para importar dados e criar o grafo Neo4j"""
    # Configurações do Neo4j (idealmente deveriam vir de variáveis de ambiente ou arquivo de configuração)
    NEO4J_URI = "neo4j://localhost:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "password"  # ATENÇÃO: Em produção, use variáveis de ambiente ou arquivo seguro
    
    # Diretório com os resultados
    RESULTADOS_DIR = "resultados"
    
    try:
        # Criar instância do gerenciador de grafo
        graph_manager = Neo4jDependencyGraph(
            uri=NEO4J_URI,
            username=NEO4J_USER,
            password=NEO4J_PASSWORD
        )
        
        # Limpar o banco de dados (opcional - remover se quiser preservar dados anteriores)
        if input("Limpar o banco de dados? (s/n): ").lower() == 's':
            graph_manager.clear_database()
        
        # Criar esquema
        graph_manager.create_schema()
        
        # Importar resultados
        total_deps = graph_manager.import_all_results(RESULTADOS_DIR)
        print(f"Total de {total_deps} dependências importadas")
        
        # Importar resultados de impacto em cascata
        total_impacts = graph_manager.import_impact_results(RESULTADOS_DIR)
        print(f"Total de {total_impacts} setores atualizados com impactos")
        
        # Executar análises
        print("\nExecutando análises do grafo...")
        analytics_results = graph_manager.run_analytics_queries()
        
        # Exportar análises para Excel
        analytics_path = os.path.join(RESULTADOS_DIR, "analises_grafo_neo4j.xlsx")
        graph_manager.export_analytics_to_excel(analytics_results, analytics_path)
        
        # Criar arquivo com consultas Cypher úteis
        queries_path = os.path.join(RESULTADOS_DIR, "consultas_cypher.md")
        create_cypher_queries_file(queries_path)
        
        # Fechar conexão
        graph_manager.close()
        
        print("\nProcessamento concluído com sucesso!")
        
    except Exception as e:
        print(f"Erro durante a execução: {str(e)}")


if __name__ == "__main__":
    main()