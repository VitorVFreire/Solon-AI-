from time import sleep, time
import investpy
import pandas as pd
import os
from dotenv import load_dotenv
import sqlite3
from utils import gerar_cnpj_completo, buscar_atividade
from tqdm import tqdm

# Carrega variáveis de ambiente
load_dotenv()

REQUESTS_PER_MINUTE = 5
SECONDS_PER_MINUTE = 65
request_timestamps = []

def manage_api_rate_limit():
    """Gerencia o limite de 5 requisições por minuto."""
    current_time = time()
    global request_timestamps
    request_timestamps = [t for t in request_timestamps if current_time - t < SECONDS_PER_MINUTE]
    
    if len(request_timestamps) >= REQUESTS_PER_MINUTE:
        sleep_time = SECONDS_PER_MINUTE - (current_time - request_timestamps[0])
        if sleep_time > 0:
            tqdm.write(f"Limite de requisições atingido. Aguardando {sleep_time:.2f} segundos...")
            sleep(sleep_time)
        request_timestamps = [t for t in request_timestamps if current_time - t < SECONDS_PER_MINUTE]
    
    request_timestamps.append(time())

def list_b3_companys(path: str, folder: str) -> pd.DataFrame:
    """Obtém lista de ações do Brasil e salva em CSV."""
    if not os.path.isdir(folder):
        os.makedirs(folder)
    try:
        stocks = investpy.get_stocks(country='brazil')
        df = pd.DataFrame(stocks)
        df.to_csv(path, sep=',', header=True, index=False)
        print(f"Lista de empresas salva em {path}")
        return df
    except Exception as e:
        print(f"Erro ao obter lista de empresas: {e}")
        return pd.DataFrame()

def classifier_companys(companys_df: pd.DataFrame, path: str, folder: str) -> pd.DataFrame:
    """Classifica empresas e salva resultados em CSV."""
    if not os.path.isdir(folder):
        os.makedirs(folder)
    
    results = []
    query_empresas = """
        SELECT *
        FROM empresas
        WHERE LOWER(
            REPLACE(REPLACE(REPLACE(REPLACE(
                razao_social,
                '/', ''), '-', ''), '&', ''), '.', '')
        ) = LOWER(
            REPLACE(REPLACE(REPLACE(REPLACE(
                ?,
                '/', ''), '-', ''), '&', ''), '.', '')
        ) LIMIT 1
    """

    try:
        conn = sqlite3.connect("cnpj.db")
        cursor = conn.cursor()
        
        for index, row in tqdm(companys_df.iterrows(), total=len(companys_df), unit=' linha', desc='Empresas Analisadas...'):
            company_name = row.get('full_name', '')
            if not company_name:
                continue
            
            tqdm.write(f'\nEmpresa Buscada: {company_name}...')
            
            try:
                cursor.execute(query_empresas, (company_name.lower(),))
                result = cursor.fetchone()
                
                if result:
                    dic = {
                        'CNPJ': '',
                        'COMPANY_NAME': company_name,
                        'ECONOMIC_ACTIVITY': [],
                        'EQUITY': ''
                    }
                    columns = [description[0] for description in cursor.description]
                    result_dict = dict(zip(columns, result))
                    dic['CNPJ'] = gerar_cnpj_completo(result_dict['numero_inscricao'])
                    
                    manage_api_rate_limit()
                    economic_activity, equity = buscar_atividade(dic['CNPJ'])
                    dic['ECONOMIC_ACTIVITY'] = economic_activity if economic_activity else []
                    dic['EQUITY'] = equity if equity else ''
                    
                    results.append(dic)
                    tqdm.write('Resultado Encontrado:')
                    tqdm.write(str(dic))
            except sqlite3.Error as e:
                tqdm.write(f"Erro ao consultar o banco para {company_name}: {e}")
            except Exception as e:
                tqdm.write(f"Erro inesperado para {company_name}: {e}")
    finally:
        conn.close()
    
    try:
        results_df = pd.DataFrame(results)
        results_df.to_csv(path, sep=',', header=True, index=False)
        print(f"\nResultados salvos em {path}")
        return results_df
    except Exception as e:
        print(f"Erro ao salvar resultados em CSV: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    path = 'database/b3_companys.csv'
    output_path = 'database/b3_companys_activities.csv'
    folder = 'database'
    
    if not os.path.exists(path):
        companys_df = list_b3_companys(path=path, folder=folder)
    else:
        try:
            companys_df = pd.read_csv(path)
            print(f"Carregando lista de empresas existente de {path}")
        except Exception as e:
            print(f"Erro ao carregar {path}: {e}")
            companys_df = pd.DataFrame()
    
    if not companys_df.empty:
        classifier_companys(companys_df, path=output_path, folder=folder)
    else:
        print("Nenhuma empresa disponível para classificação.")
