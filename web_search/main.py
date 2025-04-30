from src import *
from utils import * 
import pandas as pd
from tqdm import tqdm
import argparse

# ARGUMENTS:
parser = argparse.ArgumentParser(description="Processamento de Atividades Econômicas e Empresas...")
parser.add_argument("--max_rows", type=int, help="Quantidade de Empresas para Analisar: ")
parser.add_argument("--number_activities_create", type=int, default=10, help="Quantidade de Atividades para Criar: ")
parser.add_argument("--create_new_activities", type=bool, default=False, help="Deve ser Criado uma nova lista de Atividades? ")
parser.add_argument("--number_activities_related", type=int, help="Quantidade de Atividades para Relacionar: ")

args = parser.parse_args()

# PROMPTS:
# DETAILS ->
SYSTEM_PROMPT_DETAILS_FILE = 'prompts/details/system_prompt_details.md'
HUMAN_PROMPT_DETAILS_FILE = 'prompts/details/human_prompt_details.md'
# ACTIVITIES ->
SYSTEM_PROMPT_ACTIVITIES_FILE = 'prompts/activities/system_prompt_activities.md'
HUMAN_PROMPT_ACTIVITIES_FILE = 'prompts/activities/human_prompt_activities.md'
# ACTIVITIES_LIST ->
SYSTEM_PROMPT_ACTIVITIES_LIST_FILE = 'prompts/activities_list/system_prompt_activities_list.md'
HUMAN_PROMPT_ACTIVITIES_LIST_FILE = 'prompts/activities_list/human_prompt_activities_list.md'

# DADOS AUXILIARES:
JSON_ATIVIDADES = 'database/economic_activities.json'
CSV_COMPANYS = 'database/b3_companys.csv'

def create_activities_list(llm_client, number_activities:int = 10, batch_size: int = 30):
    """
    Cria lista de atividades econômicas
    
    Args:
        llm_client: Cliente LLM para processamento
        number_activities: Número máximo de atividades a serem criadas (padrão: 10)
        batch_size: quantidade de atividades geradas por lote
    """
    try:
        name_file = 'economic_activities'
        name_folder = 'database'
        output_path = os.path.join(name_folder, f"{name_file}.json")
        
        if os.path.exists(output_path):
            print(f"Arquivo {output_path} já existe. Pulando criação.")
            return
        
        print('Criando Lista de Atividades Econômicas...')
        processor = ActivitiesGenerate(
            llm_client=llm_client,
            system_prompt_file=SYSTEM_PROMPT_ACTIVITIES_LIST_FILE,
            human_prompt_file=HUMAN_PROMPT_ACTIVITIES_LIST_FILE,
            name_file=name_file,
            output_dir=name_folder
        )
        result = processor.process_activities(number_activities)
        print(f"Atividades Criadas, Resultado salvo em {processor.output_dir}/{name_file}.json")
    except Exception as e:
        print(f"Erro na execução de create_activities_list: {str(e)}")

def details_companies(llm_client, max_rows:int, atividades_lista: List[str]):
    """
    Processa empresas para detalhar suas caracteristicas
    
    Args:
        llm_client: Cliente LLM para processamento
        max_rows: Número máximo de empresas a serem processadas
        atividades_lista: Lista de nomes de atividades
    """
    try:
        if max_rows is not None:
            df = pd.read_csv(CSV_COMPANYS, nrows=max_rows)
        else:
            df = pd.read_csv(CSV_COMPANYS)
        
        # Usar os dados do CSV para o RAG
        empresas_data = df[['name', 'full_name', 'country', 'symbol']].to_dict('records')
        
        rag_system = EmpresasRAG(empresas_data)
        
        atividades_list = [atividade['atividade_economica'] for atividade in atividades_lista]
        
        output_dir='output/companies'
        
        path = os.listdir(output_dir)
        files = [file for file in path]
        
        # Inicializar o processador
        processor = EmpresassProcessor(
            rag_system=rag_system, 
            llm_client=llm_client, 
            system_prompt_file=SYSTEM_PROMPT_DETAILS_FILE, 
            human_prompt_file=HUMAN_PROMPT_DETAILS_FILE, 
            list_activities=atividades_list, 
            output_dir=output_dir
        )        
        # Processar cada empresa no CSV
        for _, row in tqdm(df.iterrows(), total=len(df), unit=' empresas', desc='Classificação de Empresas...'):
            company_data = row.to_dict()
            name = clean_filename(company_data['name'])
            if not f'{name}.json' in files:
                result = processor.process_company(company_data)
                tqdm.write(f"Processado: {company_data['name']} - Resultado salvo em {processor.output_dir}")
    except Exception as e:
        print(f"Erro na execução de details_companies: {str(e)}")

def related_activities(llm_client, atividades_data:List[Dict[str, Any]], atividades_lista:List[str], number_activities: int):
    """
    Processa atividades econômicas e retorna os resultados da análise.
    
    Args:
        llm_client: Cliente LLM para processamento
        atividades_data: Lista de Dicionario com dados das atividades econômicas
        atividades_lista: Lista de nomes de atividades
        number_activities: Número máximo de atividades a serem processadas (padrão: 10)
    """
    try:
        df = pd.read_json(JSON_ATIVIDADES)
        atividades_data = df['atividades_economicas']
        atividades_lista = [i['atividade_economica'] for i in atividades_data]
        
        rag_system = ActivitiesRAG(atividades_data)
        
        output_dir='output/activities'
        
        path = os.listdir(output_dir)
        files = [file for file in path]
                
        # Inicializar o processador
        processor = ActivitiesProcessor(rag_system, llm_client, SYSTEM_PROMPT_ACTIVITIES_FILE, HUMAN_PROMPT_ACTIVITIES_FILE, atividades_lista, output_dir)

        # Processar cada empresa no CSV
        for i, row in tqdm(enumerate(atividades_data), total=len(atividades_data), unit=' ativities', desc='Correlação de Atividades...'):
            name = clean_filename(row['atividade_economica'])
            if not f'{name}.json' in files:
                if number_activities is not None:
                    if i >= number_activities:
                        break
                result = processor.process_activities(row)
                tqdm.write(f"Processado: {row['atividade_economica']} - Resultado salvo em {processor.output_dir}")
    except Exception as e:
        print(f"Erro na execução de related_activities: {str(e)}")

def main():
    # Configurações
    config = setup_client('openAI')
    llm_client = AIClient(config)
    
    if not args.create_new_activities:
        # Criação de Lista de Atividades Econômicas
        create_activities_list(
            llm_client=llm_client, 
            number_activities=args.number_activities_create
        )
    
    # Carregamento da Lista de Atividades
    atividades_data, atividades_lista = load_activities(JSON_ATIVIDADES)
    
    # Detalhamento de Empresas Salvas no CSV
    details_companies(
        llm_client=llm_client, 
        max_rows=args.max_rows if args.max_rows >= 0 else None, 
        atividades_lista=atividades_lista
    )    
    
    # Criação de Correlação entre Atividades Econômicas
    related_activities(
        llm_client=llm_client, 
        number_activities=args.number_activities_related if args.number_activities_related >= 0 else None,
        atividades_data=atividades_data,
        atividades_lista=atividades_lista
    )

if __name__ == "__main__":
    main()
    #python3 main.py --number_activities_create 300 --create_new_activities True
    #python3 main.py --number_activities_create 100 --create_new_activities False --max_rows -1 --number_activities_related -1