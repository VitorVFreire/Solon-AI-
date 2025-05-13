from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from tqdm import tqdm

class ReadNews:
    def __init__(self, url: str, base_url:str, text_replace:str):
        options = webdriver.FirefoxOptions()
        options.add_argument("-headless")
        # Inicializa navegadores
        self.navegador = webdriver.Firefox(options)
        self.links = []
        self.base_url = base_url
        self.url = url
        self.text_replace = text_replace
        self.articles = []
        self.article_title = []
        self.load_links()
    
    @property
    def articles(self):
        articles = []
        for article, title in zip(self.articles, self.article_title):
            articles.append({
                "title": title,
                "article": article
            })
        return articles
        
    def load_links(self):
        # Acessa a lista de notícias
        self.navegador.get(self.url)
        time.sleep(3)

        link_elements = self.navegador.find_elements(By.XPATH, '//div[contains(@class, "px-0 md:px-6")]//a[@href]')
        for el in link_elements:
            href = el.get_attribute("href")
            if href and self.base_url in href:
                self.links.append(href)
    
    def load_article(self):
        # Percorre os links coletados
        for link in tqdm(self.links, desc='Reading News', total=len(self.links), unit='articles'):
            try:
                self.navegador.get(link)
                time.sleep(2)
                
                # Busca o artigo
                article = self.navegador.find_elements(By.XPATH, '//article')
                title  = self.navegador.find_element(By.XPATH, '//div[@data-ds-component="article-title"]')

                if article:
                    self.articles.append(article[0].text.replace(self.text_replace, ''))
                    self.article_title.append(title.text)
                    tqdm.write(f"✅ Artigo encontrado em: {link}")
                    tqdm.write(f"📰 Título: {self.article_title[-1]}")
                    #tqdm.write(f"\n=== CONTEÚDO ===\n{self.articles[-1]}\n")
                else:
                    tqdm.write(f"⚠️ Nenhum artigo encontrado em: {link}")
            
            except Exception as e:
                tqdm.write(f"❌ Erro ao acessar {link}: {e}")

        # Fecha os navegadores após o loop
        self.navegador.quit()