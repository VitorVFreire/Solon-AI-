from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options as FirefoxOptions
import time
from tqdm import tqdm
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

class ReadNews:
    def __init__(self, urls: list[str], base_url: str, text_replace: str, neo4j_conn=None, headless: bool = True):
        options = FirefoxOptions()
        if headless:
            options.add_argument("-headless")
        try:
            self.navegador = webdriver.Firefox(options=options)
            logger.info("Navegador Firefox inicializado.")
        except Exception as e:
            logger.error(f"Falha ao inicializar o Firefox. Erro: {e}")
            raise

        self.links = []
        self.base_url = base_url
        self.urls = urls
        self.text_replace = text_replace.split('|') if text_replace else []
        self.neo4j_conn = neo4j_conn
        self._articles_data: List[Dict[str, str]] = []

        self._existing_urls = self._get_existing_urls()
        self._load_links()

    @property
    def articles_list(self) -> List[Dict[str, str]]:
        seen = set()
        unique_articles = []
        for article in self._articles_data:
            if article["url"] not in seen:
                seen.add(article["url"])
                unique_articles.append(article)
        return unique_articles

    def _get_existing_urls(self) -> set:
        if not self.neo4j_conn:
            return set()
        query = "MATCH (n:News) RETURN n.url AS url"
        result = self.neo4j_conn.execute_query(query)
        return set(r["url"] for r in result if r.get("url"))

    def _load_links(self):
        for url in self.urls:
            logger.info(f"Carregando links de notícias de: {url}")
            self.navegador.get(url)
            raw_links = self.get_links(0)
            for link in raw_links:
                href = link.get_attribute('href')
                if href and url in href:
                    href = href.split('#')[0]
                    if href not in self.links:
                        self.links.append(href)

    def get_links(self, last_links: int):
        time.sleep(2)
        btn_load = None
        all_buttons = self.navegador.find_elements(By.TAG_NAME, 'button')
        links = self.navegador.find_elements(By.TAG_NAME, 'a')
        if len(links) == last_links:
            return links

        for btn in all_buttons:
            if btn.text.strip() == 'Carregar mais':
                btn_load = btn
                break
        if btn_load:
            try:
                self.navegador.execute_script("arguments[0].scrollIntoView(true);", btn_load)
                time.sleep(2)
                self.navegador.execute_script("arguments[0].click();", btn_load)
                links = self.get_links(len(links))
            except Exception as e:
                logger.error(f"Erro ao clicar em 'Carregar mais': {e}")
                return links
        return links

    def fetch_articles(self, limit: Optional[int] = None):
        logger.info(f"Iniciando busca de artigos. Links para processar: {len(self.links)}")
        if not self.links:
            logger.warning("Nenhum link para carregar artigos.")
            self.navegador.quit()
            return

        links_to_process = self.links[:limit] if limit else self.links

        for link in tqdm(links_to_process, desc='Lendo Notícias', unit='artigo'):
            try:
                self.navegador.get(link)
                time.sleep(3)

                title = "N/A"
                title_selectors = [
                    '//h1',
                    '//div[contains(@class, "title")]//h1',
                    '//header//h1',
                    '//div[@data-ds-component="article-title"]'
                ]
                for ts_xpath in title_selectors:
                    try:
                        title_element = self.navegador.find_element(By.XPATH, ts_xpath)
                        if title_element and title_element.text.strip():
                            title = title_element.text.strip()
                            break
                    except:
                        continue

                pub_date = "N/A"
                date_selectors = [
                    '//time[@datetime]',
                    '//span[contains(@class, "date")]',
                    '//div[contains(@class, "published-date")]',
                    '//meta[@property="article:published_time"]'
                ]
                for ds_xpath in date_selectors:
                    try:
                        date_element = self.navegador.find_element(By.XPATH, ds_xpath)
                        if date_element:
                            if date_element.tag_name.lower() == "meta":
                                pub_date = date_element.get_attribute("content").strip()
                            elif date_element.get_attribute("datetime"):
                                pub_date = date_element.get_attribute("datetime").strip()
                            elif date_element.text.strip():
                                pub_date = date_element.text.strip()
                            if pub_date:
                                break
                    except:
                        continue

                # --- conteúdo ---
                article_text = ""
                article_selectors = [
                    '//article',
                    '//div[contains(@class, "article-content")]',
                    '//div[@itemprop="articleBody"]'
                ]
                for as_xpath in article_selectors:
                    try:
                        article_elements = self.navegador.find_elements(By.XPATH, as_xpath)
                        if article_elements:
                            full_text = " ".join([el.text for el in article_elements if el.text])
                            for rep_text in self.text_replace:
                                full_text = full_text.replace(rep_text, '')
                            article_text = full_text.strip()
                            if article_text:
                                break
                    except:
                        continue

                if article_text and title != "N/A":
                    self._articles_data.append({
                        "title": title,
                        "article": article_text,
                        "url": link,
                        "published_date": pub_date
                    })
                    tqdm.write(f"Artigo '{title[:30]}...' encontrado em: {link} (Data: {pub_date})")
                else:
                    tqdm.write(f"Artigo ou título não identificado em: {link}. Título: '{title}', Texto: {len(article_text)} chars")

            except Exception as e:
                tqdm.write(f"Erro ao acessar ou processar {link}: {e}")

        logger.info(f"Busca de artigos concluída. {len(self._articles_data)} artigos coletados.")
        self.navegador.quit()
        logger.info("Navegador Firefox fechado.")
