import os
from dotenv import load_dotenv
from pathlib import Path
import logging
from typing import List, Dict
from src.new_scrapper import ReadNews

from neo4j import GraphDatabase

# Configuração de diretórios e variáveis de ambiente
ROOT_DIR = Path(__file__).parent
load_dotenv(override=True, dotenv_path=os.path.join(Path(__file__).parent.parent,'.env'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Atualização segura da data de publicação, ignorando URLs duplicadas
class Neo4jConnection:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def execute_query(self, query: str, params: dict = None):
        with self.driver.session() as session:
            result = session.run(query, params or {})
            return [dict(r) for r in result]

    def update_news_date(self, url: str, pub_date: str):
        query = """
        MATCH (n:News {url: $url})
        SET n.published_at = CASE 
            WHEN n.published_at IS NULL OR n.published_at = "" 
            THEN $pub_date 
            ELSE n.published_at 
        END
        RETURN n.url AS url, n.published_at AS published_at
        """
        return self.execute_query(query, {"url": url, "pub_date": pub_date})

# Filtrar URLs repetidas do mesmo artigo
def filter_unique_article_urls(urls: list[str]) -> list[str]:
    seen = set()
    filtered = []
    for url in urls:
        clean_url = url.split('#')[0]  # Remove âncoras internas
        if clean_url not in seen:
            seen.add(clean_url)
            filtered.append(clean_url)
    return filtered

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Conectar ao Neo4j
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password = os.getenv("NEO4J_PASSWORD", "senha")
    neo = Neo4jConnection(neo4j_uri, neo4j_user, neo4j_password)

    query = """
    MATCH (n:News)
    WHERE n.published_at IS NULL OR n.published_at = ""
    RETURN n.url AS url
    """
    news = neo.execute_query(query)
    urls = [n["url"].replace("#","") for n in news]  # remove #
    urls = list(set(urls))  # remove duplicatas
    logger.info(f"🔎 {len(urls)} notícias sem data encontradas")
    logger.info(f"🔎 URLs limpas e únicas: {len(urls)}")

    if urls:
        # Ler artigos com Selenium e extrair data
        reader = ReadNews(
            urls=urls,
            base_url="https://www.infomoney.com.br",
            text_replace="",
            headless=True
        )
        reader.fetch_articles()

        # Atualizar cada notícia no Neo4j
        for article in reader.articles_list:
            url = article["url"].split('#')[0]  # normaliza novamente
            pub_date = article.get("published_date", "N/A")
            if pub_date != "N/A":
                neo.update_news_date(url, pub_date)
                logger.info(f"✅ Atualizado {url} com data {pub_date}")

    neo.close()
    logger.info("🏁 Processo finalizado.")
