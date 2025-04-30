import os
import requests
import logging
import datetime
from typing import List, Dict, Any, Optional
from abc import ABC, abstractmethod
import json
import requests
from bs4 import BeautifulSoup

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NewsProvider(ABC):
    """Classe base abstrata para provedores de notícias."""
    
    @abstractmethod
    def search_news(self, query: str, limit: int = 5, language: str = "pt", days_back: int = 7) -> List[Dict[str, Any]]:
        """
        Busca notícias com base na consulta.
        
        Args:
            query: Termos de busca
            limit: Número máximo de resultados
            language: Idioma das notícias
            days_back: Quantidade de dias para buscar notícias anteriores
            
        Returns:
            Lista de notícias no formato padronizado
        """
        pass

class NewsAPIProvider(NewsProvider):
    """Implementação de provedor usando a NewsAPI."""
    
    def __init__(self, api_key: str):
        self.api_key = os.getenv("NEW_API_KEY")
        self.base_url = "https://newsapi.org/v2/everything"
    
    def search_news(self, query: str, limit: int = 5, language: str = "pt", days_back: int = 7) -> List[Dict[str, Any]]:
        try:
            # Calcular a data para o parâmetro 'from'
            from_date = (datetime.datetime.now() - datetime.timedelta(days=days_back)).strftime('%Y-%m-%d')
            
            params = {
                "q": query,
                "apiKey": self.api_key,
                "language": language,
                "sortBy": "publishedAt",
                "pageSize": limit,
                "from": from_date
            }
            
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if data.get("status") != "ok":
                logger.error(f"Erro na API NewsAPI: {data.get('message', 'Erro desconhecido')}")
                return []
            
            articles = data.get("articles", [])
            
            news_list = []
            for article in articles:
                news_list.append({
                    "title": article.get("title", ""),
                    "content": article.get("content", article.get("description", "")),
                    "published_at": article.get("publishedAt", ""),
                    "source": article.get("source", {}).get("name", ""),
                    "url": article.get("url", ""),
                    "provider": "NewsAPI"
                })
            
            return news_list
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro na requisição NewsAPI: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Erro ao processar dados da NewsAPI: {str(e)}")
            return []

class GNewsProvider(NewsProvider):
    """Implementação de provedor usando a GNews API."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://gnews.io/api/v4/search"
    
    def search_news(self, query: str, limit: int = 5, language: str = "pt", days_back: int = 7) -> List[Dict[str, Any]]:
        try:
            params = {
                "q": query,
                "token": self.api_key,
                "lang": language,
                "max": limit,
                "sortby": "publishedAt"
            }
            
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            articles = data.get("articles", [])
            
            news_list = []
            for article in articles:
                # Verificar se a notícia não é muito antiga
                published_at = article.get("publishedAt", "")
                if published_at:
                    pub_date = datetime.datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                    if (datetime.datetime.now(datetime.timezone.utc) - pub_date).days > days_back:
                        continue
                
                news_list.append({
                    "title": article.get("title", ""),
                    "content": article.get("content", article.get("description", "")),
                    "published_at": article.get("publishedAt", ""),
                    "source": article.get("source", {}).get("name", ""),
                    "url": article.get("url", ""),
                    "provider": "GNews"
                })
            
            return news_list
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro na requisição GNews: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Erro ao processar dados da GNews: {str(e)}")
            return []

class MockNewsProvider(NewsProvider):
    """Provedor de notícias simulado para testes ou quando APIs não estão disponíveis."""
    
    def __init__(self, mock_data_file: Optional[str] = None):
        """
        Inicializa o provedor simulado.
        
        Args:
            mock_data_file: Caminho opcional para um arquivo JSON com dados simulados
        """
        self.mock_data = {}
        
        if mock_data_file and os.path.exists(mock_data_file):
            try:
                with open(mock_data_file, 'r', encoding='utf-8') as f:
                    self.mock_data = json.load(f)
                logger.info(f"Dados simulados carregados de {mock_data_file}")
            except Exception as e:
                logger.error(f"Erro ao carregar dados simulados: {str(e)}")
    
    def search_news(self, query: str, limit: int = 5, language: str = "pt", days_back: int = 7) -> List[Dict[str, Any]]:
        """
        Retorna notícias simuladas para a consulta.
        
        Args:
            query: Termos de busca
            limit: Número máximo de resultados
            language: Idioma das notícias (ignorado)
            days_back: Quantidade de dias (ignorado)
            
        Returns:
            Lista de notícias simuladas
        """
        # Buscar notícias específicas para a consulta, se existirem
        if query.lower() in self.mock_data:
            news = self.mock_data[query.lower()]
            return news[:limit]
        
        # Gerar notícias simuladas
        current_date = datetime.datetime.now()
        
        # Notícias simuladas genéricas
        news_list = [
            {
                "title": f"Notícia simulada sobre {query} - Desenvolvimento recente",
                "content": f"Esta é uma notícia simulada sobre {query}. A empresa anunciou novos desenvolvimentos que podem impactar significativamente o mercado nos próximos meses.",
                "published_at": current_date.isoformat(),
                "source": "Simulação Econômica",
                "url": "https://exemplo.com/noticias-simuladas",
                "provider": "MockNews"
            },
            {
                "title": f"Análise financeira de {query} mostra tendências positivas",
                "content": f"Uma análise recente sobre {query} indica tendências positivas para o próximo trimestre, com expectativa de crescimento acima da média do setor.",
                "published_at": (current_date - datetime.timedelta(days=1)).isoformat(),
                "source": "Finanças Simuladas",
                "url": "https://exemplo.com/financas-simuladas",
                "provider": "MockNews"
            },
            {
                "title": f"Mercado reage a anúncio de {query}",
                "content": f"O mercado reagiu positivamente ao anúncio feito por {query} sobre sua nova estratégia de expansão internacional. Analistas preveem aumento no valor das ações.",
                "published_at": (current_date - datetime.timedelta(days=2)).isoformat(),
                "source": "Mercado Simulado",
                "url": "https://exemplo.com/mercado-simulado",
                "provider": "MockNews"
            },
            {
                "title": f"Relatório trimestral de {query} supera expectativas",
                "content": f"O relatório trimestral de {query} superou as expectativas dos analistas, com crescimento de 15% em relação ao mesmo período do ano anterior.",
                "published_at": (current_date - datetime.timedelta(days=3)).isoformat(),
                "source": "Economia Digital",
                "url": "https://exemplo.com/economia-digital",
                "provider": "MockNews"
            },
            {
                "title": f"Crise afeta operações de {query} em mercados emergentes",
                "content": f"A recente crise econômica tem afetado as operações de {query} em mercados emergentes, levando a uma revisão das projeções para o próximo ano fiscal.",
                "published_at": (current_date - datetime.timedelta(days=5)).isoformat(),
                "source": "Crises Simuladas",
                "url": "https://exemplo.com/crises-simuladas",
                "provider": "MockNews"
            }
        ]
        
        return news_list[:limit]

class WebScraperProvider(NewsProvider):
    """Implementação de provedor usando web scraping (básico)."""
    
    def __init__(self, search_url: str = "https://www.google.com/search"):
        """
        Inicializa o provedor de web scraping.
        
        Args:
            search_url: URL base para buscas
        """
        self.search_url = search_url
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
    
    def search_news(self, query: str, limit: int = 5, language: str = "pt", days_back: int = 7) -> List[Dict[str, Any]]:
        """
        NOTA: Esta é uma implementação básica e limitada. Para uso em produção,
        seria necessário desenvolver um scraper mais robusto e específico para sites de notícias.
        """
        try:            
            # Adicionar termo "notícias" à consulta
            search_query = f"{query} notícias"
            if language == "pt":
                search_query += " finanças economia"
            elif language == "en":
                search_query += " finance economy news"
                
            # Adicionar parâmetro de tempo
            time_range = f"qdr:d{days_back}"
            
            params = {
                "q": search_query,
                "tbm": "nws",  # Buscar na seção de notícias
                "hl": language,
                "tbs": time_range
            }
            
            response = requests.get(self.search_url, params=params, headers=self.headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extrair resultados de notícias (isso varia dependendo da estrutura HTML do site)
            # Esta é uma implementação simplificada e pode precisar de ajustes
            news_elements = soup.select('div.g')[:limit]
            
            news_list = []
            for element in news_elements:
                title_element = element.select_one('h3')
                link_element = element.select_one('a')
                snippet_element = element.select_one('.st')
                source_element = element.select_one('.UPmit')
                
                if title_element and link_element:
                    title = title_element.text
                    url = link_element['href']
                    content = snippet_element.text if snippet_element else ""
                    source = source_element.text if source_element else "Unknown Source"
                    
                    news_list.append({
                        "title": title,
                        "content": content,
                        "published_at": datetime.datetime.now().isoformat(),  # Data não disponível diretamente
                        "source": source,
                        "url": url,
                        "provider": "WebScraper"
                    })
            
            return news_list
        except ImportError:
            logger.error("Bibliotecas necessárias não instaladas: requests, bs4")
            return []
        except Exception as e:
            logger.error(f"Erro na busca por web scraping: {str(e)}")
            return []

class NewsService:
    """Serviço central para busca de notícias, gerenciando múltiplos provedores."""
    
    def __init__(self):
        """Inicializa o serviço de notícias com provedores configurados."""
        self.providers = []
        self._setup_providers()
    
    def _setup_providers(self):
        """Configura os provedores de notícias com base nas variáveis de ambiente."""
        # NewsAPI
        newsapi_key = os.getenv("NEWS_API_KEY")
        if newsapi_key:
            self.providers.append(NewsAPIProvider(newsapi_key))
            logger.info("NewsAPI configurado")
        
        # GNews
        gnews_key = os.getenv("GNEWS_API_KEY")
        if gnews_key:
            self.providers.append(GNewsProvider(gnews_key))
            logger.info("GNews configurado")
        
        # Se não houver provedores de API configurados, usar o provedor simulado
        if not self.providers:
            logger.warning("Nenhum provedor de API configurado. Usando provedor simulado.")
            mock_data_file = os.getenv("MOCK_NEWS_FILE", "data/mock_news.json")
            self.providers.append(MockNewsProvider(mock_data_file))
        
        # Adicionar Web Scraper como fallback (se nenhum resultado for encontrado nas APIs)
        try:
            import bs4
            self.providers.append(WebScraperProvider())
            logger.info("WebScraper configurado como fallback")
        except ImportError:
            logger.warning("BeautifulSoup não instalado. WebScraper não disponível.")
    
    def search_news(self, query: str, limit: int = 5, language: str = "pt", days_back: int = 7) -> List[Dict[str, Any]]:
        """
        Busca notícias em todos os provedores configurados.
        
        Args:
            query: Termos de busca
            limit: Número máximo de resultados
            language: Idioma das notícias
            days_back: Quantidade de dias para buscar notícias anteriores
            
        Returns:
            Lista combinada de notícias de todos os provedores
        """
        all_news = []
        
        # Dividir o limite entre os provedores
        provider_limit = max(2, limit // len(self.providers))
        
        for provider in self.providers:
            try:
                # Buscar notícias do provedor
                news = provider.search_news(query, provider_limit, language, days_back)
                if news:
                    all_news.extend(news)
                    logger.info(f"Encontradas {len(news)} notícias de {provider.__class__.__name__}")
            except Exception as e:
                logger.error(f"Erro ao buscar notícias de {provider.__class__.__name__}: {str(e)}")
        
        # Remover duplicatas (baseado no título)
        unique_news = []
        seen_titles = set()
        
        for news in all_news:
            title = news.get("title", "").lower()
            if title and title not in seen_titles:
                seen_titles.add(title)
                unique_news.append(news)
        
        # Ordenar por data (mais recentes primeiro)
        try:
            unique_news.sort(
                key=lambda x: datetime.datetime.fromisoformat(
                    x.get("published_at", "").replace("Z", "+00:00")
                ) if x.get("published_at") else datetime.datetime.min,
                reverse=True
            )
        except Exception as e:
            logger.error(f"Erro ao ordenar notícias por data: {str(e)}")
        
        # Retornar até o limite solicitado
        return unique_news[:limit]
    
    def save_news_to_file(self, news_list: List[Dict[str, Any]], company_name: str, output_dir: str = "data/news"):
        """
        Salva a lista de notícias em um arquivo JSON.
        
        Args:
            news_list: Lista de notícias para salvar
            company_name: Nome da empresa
            output_dir: Diretório para salvar o arquivo
        """
        if not news_list:
            return
        
        try:
            os.makedirs(output_dir, exist_ok=True)
            
            # Limpar o nome da empresa para usar como nome de arquivo
            from utils import clean_filename
            safe_name = clean_filename(company_name)
            
            # Adicionar timestamp ao nome do arquivo
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{safe_name}_news_{timestamp}.json"
            
            # Caminho completo do arquivo
            filepath = os.path.join(output_dir, filename)
            
            # Adicionar metadados
            data = {
                "company_name": company_name,
                "fetched_at": datetime.datetime.now().isoformat(),
                "news_count": len(news_list),
                "news": news_list
            }
            
            # Salvar em JSON
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                
            logger.info(f"Notícias salvas em {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Erro ao salvar notícias: {str(e)}")
            return None