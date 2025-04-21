import os
import requests
from bs4 import BeautifulSoup
from zipfile import ZipFile
from io import BytesIO
from tqdm import tqdm
import pandas as pd
import chardet

def deletar_arquivos(target_path: str):
    if os.path.exists(target_path):
        for arquivo in os.listdir(target_path):
            try:
                os.remove(os.path.join(target_path, arquivo))
            except Exception as e:
                print(f'Erro ao deletar {arquivo}: {e}')
        print('Arquivos deletados.')

def baixar_empresas(BASE_URL: str, target_path: str, name_file: str):
    os.makedirs(target_path, exist_ok=True)

    res = requests.get(BASE_URL)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, 'html.parser')

    links = [a['href'] for a in soup.find_all('a', href=True) if a['href'].startswith(name_file) and a['href'].endswith('.zip')]

    print(f'Encontrados {len(links)} arquivos para download.')

    for nome_arquivo in tqdm(links, desc=f'Baixando Arquivos {name_file}...', unit='Files'):
        url_completa = BASE_URL + nome_arquivo
        resposta = requests.get(url_completa)
        resposta.raise_for_status()

        with ZipFile(BytesIO(resposta.content)) as zip_ref:
            zip_ref.extractall(target_path)
    print('Arquivos baixados e extraídos.')

def detectar_encoding(caminho_arquivo, num_bytes=10000):
    with open(caminho_arquivo, 'rb') as f:
        rawdata = f.read(num_bytes)
    result = chardet.detect(rawdata)
    encoding = result['encoding']
    if encoding is None or encoding.lower() == 'ascii' or encoding.lower() == 'utf-8':
        return 'ISO-8859-1'
    return encoding

def converter_para_csv_utf8(target_path: str):
    for nome_antigo in tqdm(os.listdir(target_path), desc=f'Convertendo Arquivos de {target_path}...', unit='Files'):
        caminho_antigo = os.path.join(target_path, nome_antigo)

        if os.path.isfile(caminho_antigo):
            nome_base, _ = os.path.splitext(nome_antigo)
            nome_novo = nome_base + '.csv'
            caminho_novo = os.path.join(target_path, nome_novo)

            # Detectar encoding original
            encoding_original = detectar_encoding(caminho_antigo)

            try:
                df = pd.read_csv(caminho_antigo, sep=';', encoding=encoding_original, header=None, dtype=str)
                df.to_csv(caminho_novo, index=False, header=False, sep=';', encoding='utf-8')
                os.remove(caminho_antigo)
            except Exception as e:
                print(f"Erro ao converter {nome_antigo}: {e}")
    print('Arquivos convertidos para UTF-8.')

def compactar(path: str, sql_table: str, sql_file: str, cabecalho_padrao: list, chunk_size: int = 10000):
    arquivos_csv = [f for f in os.listdir(path) if f.lower().endswith('.csv')]

    # Primeiro, verificar a estrutura dos arquivos
    num_colunas_real = 0
    for arquivo in arquivos_csv[:1]:  # Verificar apenas o primeiro arquivo
        caminho = os.path.join(path, arquivo)
        try:
            amostra = pd.read_csv(caminho, sep=';', encoding='utf-8', dtype=str, header=None, nrows=1)
            num_colunas_real = len(amostra.columns)
            print(f"Arquivo {arquivo} tem {num_colunas_real} colunas")
        except Exception as e:
            print(f"Erro ao verificar estrutura de {arquivo}: {e}")
    
    # Usar o número real de colunas encontrado nos arquivos
    cabecalho_atual = cabecalho_padrao[:num_colunas_real]
    print(f"Usando o cabeçalho: {cabecalho_atual}")

    with open(sql_file, 'w', encoding='utf-8') as f_sql:
        for arquivo in tqdm(arquivos_csv, desc="Processando arquivos CSV", unit='Files'):
            caminho = os.path.join(path, arquivo)

            try:
                for chunk in pd.read_csv(caminho, sep=';', encoding='utf-8', dtype=str, header=None, chunksize=chunk_size):
                    # Garantir que usamos apenas as colunas que existem
                    chunk.columns = range(len(chunk.columns))
                    chunk = chunk.iloc[:, :num_colunas_real]
                    chunk.columns = cabecalho_atual
                    
                    for _, row in chunk.iterrows():
                        valores = "', '".join(str(v).replace("'", "''") for v in row)
                        colunas = ", ".join(cabecalho_atual)
                        linha = f"INSERT INTO {sql_table} ({colunas}) VALUES ('{valores}');"
                        f_sql.write(linha + '\n')
            except Exception as e:
                print(f"Erro ao processar {arquivo}: {e}")
                continue

    print(f"Arquivo SQL gerado: {sql_file}")

if __name__ == "__main__":
    os.makedirs('sql', exist_ok=True)
    BASE_URL = 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-03/'
    
    print('Empresas...')
    target_path = 'empresas'
    #deletar_arquivos(target_path)
    #baixar_empresas(BASE_URL, target_path, 'Empresas')
    #converter_para_csv_utf8(target_path)

    cabecalho_empresas = [
        'numero_inscricao',
        'razao_social',
        'codigo_atividade',
        'codigo_municipio',
        'capital_social',
        'natureza_juridica',
        'coluna_vazia_1'
    ]

    compactar(target_path, 'empresas', 'sql/empresas.sql', cabecalho_empresas)
    
    print('CNAES...')
    target_path = 'cnaes'
    #deletar_arquivos(target_path)
    #baixar_empresas(BASE_URL, target_path, 'Cnaes')
    #converter_para_csv_utf8(target_path)
    
    cabecalho_cnaes = [
        'codigo_atividade',
        'nome_atividade',
        'coluna_vazia_2'
    ]

    compactar(target_path, 'cnaes', 'sql/cnaes.sql', cabecalho_cnaes)