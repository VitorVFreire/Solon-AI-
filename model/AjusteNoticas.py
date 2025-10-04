import os
from dotenv import load_dotenv
from pathlib import Path
import logging
from typing import List
from src.new_scrapper import ReadNews
from googletrans import Translator
from neo4j import GraphDatabase
import uuid

ROOT_DIR = Path(__file__).parent
load_dotenv(override=True, dotenv_path=os.path.join(Path(__file__).parent.parent, '.env'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def filter_unique_article_urls(urls: list[str]) -> list[str]:
    seen = set()
    filtered = []
    for url in urls:
        clean_url = url.split('#')[0]
        if clean_url not in seen:
            seen.add(clean_url)
            filtered.append(clean_url)
    return filtered

def translate_text(text: str, dest: str = 'pt') -> str:
    if not text or text.strip() == "":
        return text
    try:
        translator = Translator()
        translated = translator.translate(text, dest=dest)
        return translated.text
    except Exception as e:
        logger.warning(f"Falha na tradução: {e}")
        return text


def translate_analysis_fields(neo: Neo4jConnection, dest: str = "pt"):
    query_fetch = """
    MATCH (a:Analysis)
    WHERE a.justification_personal IS NOT NULL OR a.justification_sector IS NOT NULL
    RETURN a.analysis_id AS analysis_id, a.justification_personal AS personal, a.justification_sector AS sector
    """
    analysis_nodes = neo.execute_query(query_fetch)
    logger.info(f"Encontrados {len(analysis_nodes)} nós Analysis para tradução")

    for node in analysis_nodes:
        node_id = node["analysis_id"]
        personal_text = node.get("personal")
        sector_text = node.get("sector")

        translated_personal = translate_text(personal_text, dest=dest) if personal_text else None
        translated_sector = translate_text(sector_text, dest=dest) if sector_text else None

        query_update = """
        MATCH (a:Analysis {analysis_id: $analysis_id})
        SET a.justification_personal = COALESCE($personal, a.justification_personal),
            a.justification_sector = COALESCE($sector, a.justification_sector)
        """
        neo.execute_query(query_update, {
            "analysis_id": node_id,
            "personal": translated_personal,
            "sector": translated_sector
        })
        logger.info(f"Nó Analysis {node_id} atualizado com tradução")

if __name__ == "__main__":
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password = os.getenv("NEO4J_PASSWORD", "senha")
    neo = Neo4jConnection(neo4j_uri, neo4j_user, neo4j_password)

    analysis_nodes = neo.execute_query("MATCH (a:Analysis) WHERE a.analysis_id IS NULL RETURN a")
    for node in analysis_nodes:
        new_id = str(uuid.uuid4())
        neo.execute_query("""
        MATCH (a:Analysis)
        WHERE a.analysis_id IS NULL AND a.created_at = $created_at
        SET a.analysis_id = $new_id
        """, {"created_at": node['a']['created_at'], "new_id": new_id})

    query = """
    MATCH (n:News)
    WHERE n.published_at IS NULL OR n.published_at = ""
    RETURN n.url AS url
    """
    news = neo.execute_query(query)
    urls = [n["url"].split('#')[0] for n in news]
    urls = filter_unique_article_urls(urls)
    logger.info(f"🔎 {len(urls)} notícias sem data encontradas")
    logger.info(f"🔎 URLs limpas e únicas: {len(urls)}")

    if urls:
        reader = ReadNews(
            urls=urls,
            base_url="https://www.infomoney.com.br",
            text_replace="",
            headless=True
        )
        reader.fetch_articles()
        for article in reader.articles_list:
            url = article["url"].split('#')[0]
            pub_date = article.get("published_date", "N/A")
            if pub_date != "N/A":
                neo.update_news_date(url, pub_date)
                logger.info(f"✅ Atualizado {url} com data {pub_date}")

    translate_analysis_fields(neo, dest="pt")

    neo.close()
    logger.info("Processo finalizado.")
