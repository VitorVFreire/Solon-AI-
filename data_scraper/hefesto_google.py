import os
from dotenv import load_dotenv
from newsapi import NewsApiClient

# Carregar variáveis de ambiente
load_dotenv()
API_KEY = os.getenv("API_KEY")

if not API_KEY:
    raise ValueError("A API_KEY não foi encontrada. Verifique o arquivo .env")

# Inicializar cliente da NewsAPI
newsapi = NewsApiClient(api_key=API_KEY)

# Obter principais manchetes (sem sources para evitar conflito com country/category)
top_headlines = newsapi.get_top_headlines(
    q='bitcoin',
    category='business',  # Mantendo category
    language='en',
    country='us'  # Mantendo country
)

# Obter todos os artigos relacionados ao tema
all_articles = newsapi.get_everything(
    q='bitcoin',
    sources='bbc-news,the-verge',
    domains='bbc.co.uk,techcrunch.com',
    from_param='2025-02-25',
    to='2025-03-25',
    language='pt',
    sort_by='relevancy',
    page=2
)

# Obter fontes disponíveis
sources = newsapi.get_sources()

# Exibir resultados para verificar
print("Top Headlines:", top_headlines)
print("All Articles:", all_articles)
print("Sources:", sources)