from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# from webdriver_manager.firefox import GeckoDriverManager # Optional: for auto-driver management
import time
from tqdm import tqdm
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

class ReadNews:
    def __init__(self, urls: list[str], base_url:str, text_replace:str, headless: bool = True):
        options = FirefoxOptions()
        if headless:
            options.add_argument("-headless")
        
        # For local WebDriver:
        # Ensure geckodriver is in your PATH or specify its path:
        # service = FirefoxService(executable_path='/path/to/geckodriver')
        # self.navegador = webdriver.Firefox(service=service, options=options)
        try:
            # Using webdriver_manager (install with: pip install webdriver-manager)
            # self.navegador = webdriver.Firefox(service=FirefoxService(GeckoDriverManager().install()), options=options)
            # Or, if geckodriver is in PATH:
            self.navegador = webdriver.Firefox(options=options)
            logger.info("Navegador Firefox inicializado.")
        except Exception as e:
            logger.error(f"Falha ao inicializar o Firefox. Verifique se o geckodriver está no PATH ou instale 'webdriver-manager'. Erro: {e}")
            raise

        self.links = []
        self.base_url = base_url
        self.urls = urls
        self.text_replace = text_replace.split('|') if text_replace else [] # Allow multiple replacements
        self._articles_data: List[Dict[str, str]] = []
        self._load_links() # Load links on init

    @property
    def articles_list(self) -> List[Dict[str, str]]:
        return list(dict.fromkeys(self._articles_data))

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
                        "url": link
                    })
                    tqdm.write(f"✅ Artigo '{title[:30]}...' encontrado em: {link}")
                else:
                    tqdm.write(f"⚠️ Artigo ou título não claramente identificado em: {link}. Título: '{title}', Texto encontrado: {len(article_text)} chars.")
            
            except Exception as e:
                tqdm.write(f"❌ Erro ao acessar ou processar {link}: {e}")

        logger.info(f"Busca de artigos concluída. {len(self._articles_data)} artigos coletados.")
        self.navegador.quit()
        logger.info("Navegador Firefox fechado.")
        
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
        
    def _load_links(self):
        for url in self.urls:
            logger.info(f"Carregando links de notícias de: {url}")
            self.navegador.get(url)
            raw_links = self.get_links(0)
            self.links.extend([link.get_attribute('href') for link in raw_links if link.get_attribute('href') and url in link.get_attribute('href')])                