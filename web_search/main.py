from src import *
import pandas as pd
from tqdm import tqdm

# PROMPTS:
SYSTEM_PROMP_FILE = 'prompts/system_prompt.md'
HUMAN_PROMPT_FILE = 'prompts/human_prompt.md'

# DADOS AUXILIARES:
CSV_ATIVIDADES = 'database/economic_activities.csv'
CSV_COMPANYS = 'database/b3_companys.csv' 

def main(max_rows: int = None):
    """
    Processa empresas de um CSV, com opção de limitar o número de linhas.

    Args:
        max_rows (int, optional): Número máximo de linhas a processar. Se None, processa todas.
    """
    # Configurações
    config = setup_xai_client()
    llm_client = XAIClient(config)
    
    if max_rows is not None:
        df = pd.read_csv(CSV_COMPANYS, nrows=max_rows)  # Limitar linhas ao ler o CSV
    else:
        df = pd.read_csv(CSV_COMPANYS)  # Ler todo o CSV
    
    # Usar os dados do CSV para o RAG
    empresas_data = df[['name', 'full_name', 'country', 'symbol']].to_dict('records')
    
    rag_system = EmpresasRAG(empresas_data)
    
    atividades_data = pd.read_csv(CSV_ATIVIDADES)

    # Extrair apenas a coluna 'atividade_economica' como uma lista
    atividades_lista = atividades_data['atividade_economica'].tolist()
    
    # Inicializar o processador
    processor = EmpresassProcessor(rag_system, llm_client, SYSTEM_PROMP_FILE, HUMAN_PROMPT_FILE, atividades_lista, 'output')
    
    # Processar cada empresa no CSV
    for _, row in tqdm(df.iterrows(), total=len(df), unit=' empresas', desc='Classificação de Empresas...'):
        company_data = row.to_dict()
        result = processor.process_company(company_data)
        tqdm.write(f"Processado: {company_data['name']} - Resultado salvo em {processor.output_dir}")

if __name__ == "__main__":
    main(max_rows=3)