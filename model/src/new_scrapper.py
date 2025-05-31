from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
# from webdriver_manager.firefox import GeckoDriverManager # Optional: for auto-driver management
import time
from tqdm import tqdm
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

class ReadNews:
    def __init__(self, url: str, base_url:str, text_replace:str, headless: bool = True):
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
        self.url = url
        self.text_replace = text_replace.split('|') if text_replace else [] # Allow multiple replacements
        self._articles_data: List[Dict[str, str]] = []
        self._load_links() # Load links on init

    @property
    def articles_list(self) -> List[Dict[str, str]]:
        return self._articles_data
        
    def _load_links(self):
        logger.info(f"Carregando links de notícias de: {self.url}")
        try:
            self.navegador.get(self.url)
            time.sleep(3)
            added_links = 0
            link_elements = self.navegador.find_elements(By.XPATH, '//div[contains(@class, "px-0 md:px-6")]//a[@href]')
            for el in link_elements:
                href = el.get_attribute("href")
                if href and self.base_url in href:
                    self.links.append(href)
                    added_links +=1
            logger.info(f"{added_links} links de artigos potencialmente relevantes adicionados de {self.url}. Total: {len(self.links)}.")
            if not self.links:
                logger.warning(f"Nenhum link correspondente à base_url '{self.base_url}' encontrado em '{self.url}'. Verifique os seletores ou a página.")

        except Exception as e:
            logger.error(f"Erro ao carregar links de {self.url}: {e}")

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
                time.sleep(3) # Wait for page to load
                
                # Try to find title (common patterns)
                title = "N/A"
                title_selectors = [
                    '//h1', 
                    '//div[contains(@class, "title")]//h1', 
                    '//header//h1',
                    '//div[@data-ds-component="article-title"]' # Original
                ]
                for ts_xpath in title_selectors:
                    try:
                        title_element = self.navegador.find_element(By.XPATH, ts_xpath)
                        if title_element and title_element.text.strip():
                            title = title_element.text.strip()
                            break
                    except: # pylint: disable=bare-except
                        continue
                
                # Try to find article body (common patterns)
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
                            for rep_text in self.text_replace: # Handle multiple replacements
                                full_text = full_text.replace(rep_text, '')
                            article_text = full_text.strip()
                            if article_text: # Found good article text
                                break 
                    except: # pylint: disable=bare-except
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