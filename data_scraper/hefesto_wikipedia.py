import pandas as pd
import requests
from bs4 import BeautifulSoup
from googlesearch import search
import time

# Função para buscar o link da Wikipédia no Google
def get_wikipedia_link(company_name):
    try:
        query = f"{company_name} site:*.wikipedia.org"
        for url in search(query, num_results=1, sleep_interval=2):
            if "wikipedia.org" in url:
                return url
        return None
    except Exception as e:
        print(f"Erro ao buscar {company_name}: {e}")
        return None

# Função para extrair a informação de "Atividade" da página da Wikipédia
def get_wikipedia_activity(wiki_url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(wiki_url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Localizar a tabela de informações (infobox)
        infobox = soup.find('table', class_='infobox')
        if not infobox:
            return None
        
        # Procurar a linha com "Atividade" ou "Industry"
        for row in infobox.find_all('tr'):
            header = row.find('th')
            if header and ('Atividade' in header.text or 'Industry' in header.text):
                value = row.find('td')
                if value:
                    return value.text.strip()
        return None
    except Exception as e:
        print(f"Erro ao extrair atividade de {wiki_url}: {e}")
        return None

# Carregar o CSV
input_csv = 'database/b3_companys_activities.csv'
df = pd.read_csv(input_csv)

# Adicionar nova coluna para armazenar a atividade
df['Atividade Wikipédia'] = None

# Percorrer cada linha da coluna company_name
for index, row in df.iterrows():
    company_name = row['COMPANY_NAME']
    print(f"Processando: {company_name}")
    
    # Buscar o link da Wikipédia
    wiki_url = get_wikipedia_link(company_name)
    if wiki_url:
        # Extrair a informação de atividade
        activity = get_wikipedia_activity(wiki_url)
        if activity:
            df.at[index, 'Atividade Wikipédia'] = activity
    
    # Pausa para evitar bloqueio
    time.sleep(2)

# Salvar o novo CSV com a coluna adicional
output_csv = 'database/b3_companys_activities_with_activity.csv'
df.to_csv(output_csv, index=False)
print(f"Arquivo salvo como: {output_csv}")