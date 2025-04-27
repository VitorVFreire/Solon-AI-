from io import StringIO
from src import *
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
    """
    try:
        name_file = 'economic_activities'
        name_folder = 'database'
        if not os.path.exists(f'{name_folder}/{name_file}.json') or args.create_new_activities:
            print('Criando Lista de Atividades Econômicas...')
            processor = CreateActivities(
                llm_client=llm_client,
                system_prompt_file=SYSTEM_PROMPT_ACTIVITIES_LIST_FILE,
                human_prompt_file=HUMAN_PROMPT_ACTIVITIES_LIST_FILE,
                name_file=name_file,
                output_dir=name_folder
            )
            
            result = processor.process_activities(number_activities, batch_size=batch_size)
            print(f"Atividades Criadas, Resultado salvo em {processor.output_dir}")
    except Exception as e:
        print(f"Erro na execução de create_activities_list: {str(e)}")

def details_companys(llm_client, max_rows:int):
    """
    Processa empresas para detalhar suas caracteristicas
    
    Args:
        llm_client: Cliente LLM para processamento
        max_rows: Número máximo de empresas a serem processadas
    """
    try:
        if max_rows is not None:
            df = pd.read_csv(CSV_COMPANYS, nrows=max_rows)
        else:
            df = pd.read_csv(CSV_COMPANYS)
        
        # Usar os dados do CSV para o RAG
        empresas_data = df[['name', 'full_name', 'country', 'symbol']].to_dict('records')
        
        rag_system = EmpresasRAG(empresas_data)
        
        atividades_data = pd.read_json(JSON_ATIVIDADES)

        atividades_lista = [i['atividade_economica'] for i in atividades_data['atividades_economicas']]
        
        # Inicializar o processador
        processor = EmpresassProcessor(
            rag_system=rag_system, 
            llm_client=llm_client, 
            system_prompt_file=SYSTEM_PROMPT_DETAILS_FILE, 
            human_prompt_file=HUMAN_PROMPT_DETAILS_FILE, 
            list_activities=atividades_lista, 
            output_dir='output/companys'
        )
        
        # Processar cada empresa no CSV
        for _, row in tqdm(df.iterrows(), total=len(df), unit=' empresas', desc='Classificação de Empresas...'):
            company_data = row.to_dict()
            result = processor.process_company(company_data)
            tqdm.write(f"Processado: {company_data['name']} - Resultado salvo em {processor.output_dir}")
    except Exception as e:
        print(f"Erro na execução de details_companys: {str(e)}")

def related_activities(llm_client, number_activities: int = 10):
    """
    Processa atividades econômicas e retorna os resultados da análise.
    
    Args:
        llm_client: Cliente LLM para processamento
        number_activities: Número máximo de atividades a serem processadas (padrão: 10)
    """
    try:
        df = pd.read_json(JSON_ATIVIDADES)
        atividades_data = df['atividades_economicas']
        atividades_lista = [i['atividade_economica'] for i in atividades_data]
        
        rag_system = ActivitiesRAG(atividades_data)
        
        # Inicializar o processador
        processor = ActivitiesProcessor(rag_system, llm_client, SYSTEM_PROMPT_ACTIVITIES_FILE, HUMAN_PROMPT_ACTIVITIES_FILE, atividades_lista,'output/activities')

        # Processar cada empresa no CSV
        for i, row in tqdm(enumerate(atividades_data), total=len(atividades_data), unit=' ativities', desc='Correlação de Atividades...'):
            if i >= number_activities:
                break
            result = processor.process_activities(row)
            tqdm.write(f"Processado: {row['atividade_economica']} - Resultado salvo em {processor.output_dir}")
    except Exception as e:
        print(f"Erro na execução de related_activities: {str(e)}")
    
def main():
    # Configurações
    config = setup_xai_client()
    llm_client = XAIClient(config)
    
    #create_activities_list(llm_client=llm_client, number_activities=args.number_activities_create) 
    details_companys(llm_client=llm_client, max_rows=args.max_rows)    
    related_activities(llm_client=llm_client, number_activities=args.number_activities_related)

if __name__ == "__main__":
    main()