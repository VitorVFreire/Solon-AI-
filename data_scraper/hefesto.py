from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from dotenv import load_dotenv
import os
import time
import pickle
import random
import re
import datetime

# Carregar variáveis de ambiente
load_dotenv()

EMAIL = os.getenv("EMAIL")
PASSWORD = os.getenv("PASSWORD")

if not EMAIL or not PASSWORD:
    raise ValueError("As credenciais não foram carregadas corretamente. Verifique seu arquivo .env.")

# Configuração do navegador
def setup_driver(headless=True):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    
    # Adicionar user-agent para simular navegador real
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Desativar detecção de automação
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    
    return webdriver.Chrome(options=options)

# Gerenciamento de cookies
def save_cookies(driver, path):
    with open(path, "wb") as file:
        pickle.dump(driver.get_cookies(), file)
    print(f"Cookies salvos em {path}")

def load_cookies(driver, path):
    try:
        with open(path, "rb") as file:
            cookies = pickle.load(file)
            for cookie in cookies:
                driver.add_cookie(cookie)
        print(f"Cookies carregados de {path}")
        return True
    except Exception as e:
        print(f"Erro ao carregar cookies: {e}")
        return False

def login(driver, email, password):
    """Função de login com tratamento de erros e verificações"""
    try:
        print("Iniciando processo de login...")
        driver.get("https://x.com/login")
        
        # Aguardar campo de email e preencher
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.NAME, "text")))
        username_input = driver.find_element(By.NAME, "text")
        username_input.clear()
        type_with_random_delay(username_input, email)
        username_input.send_keys(Keys.RETURN)
        
        # Verificar se há solicitação de verificação adicional
        try:
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'Verificar')]")))
            print("Etapa de verificação detectada. Processando...")
            # Adicione lógica de verificação aqui se necessário
        except TimeoutException:
            pass
        
        # Aguardar campo de senha e preencher
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.NAME, "password")))
        password_input = driver.find_element(By.NAME, "password")
        password_input.clear()
        type_with_random_delay(password_input, password)
        password_input.send_keys(Keys.RETURN)
        
        # Verificar login bem-sucedido
        try:
            WebDriverWait(driver, 20).until(EC.url_contains("home"))
            print("Login realizado com sucesso!")
            return True
        except TimeoutException:
            # Verificar mensagens de erro
            error_messages = driver.find_elements(By.XPATH, "//*[contains(text(), 'incorreta') or contains(text(), 'falhou') or contains(text(), 'erro')]")
            if error_messages:
                print(f"Erro no login: {error_messages[0].text}")
            else:
                print("Login falhou por razão desconhecida.")
            return False
            
    except Exception as e:
        print(f"Erro durante o login: {e}")
        return False

def type_with_random_delay(element, text):
    """Simula digitação humana com delays aleatórios entre teclas"""
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(0.05, 0.2))

def extract_tweet_data(tweet_element):
    """Extrai informações detalhadas de um elemento de tweet"""
    try:
        # Extrair nome e username
        user_info = tweet_element.find_elements(By.XPATH, ".//span[contains(@class, 'css-')]")
        username = user_info[1].text if len(user_info) > 1 else "Desconhecido"
        
        # Extrair texto do tweet
        tweet_text_elements = tweet_element.find_elements(By.XPATH, ".//div[@data-testid='tweetText']")
        tweet_text = tweet_text_elements[0].text if tweet_text_elements else ""
        
        # Extrair timestamp
        timestamp_elements = tweet_element.find_elements(By.XPATH, ".//time")
        timestamp = timestamp_elements[0].get_attribute("datetime") if timestamp_elements else ""
        
        # Extrair métricas (likes, retweets, etc)
        metrics = {}
        metric_elements = tweet_element.find_elements(By.XPATH, ".//div[@role='group']//span[@data-testid='app-text-transition-container']")
        
        # Tentar extrair URL do tweet
        try:
            link_elements = tweet_element.find_elements(By.XPATH, ".//a[contains(@href, '/status/')]")
            tweet_url = link_elements[0].get_attribute("href") if link_elements else ""
        except:
            tweet_url = ""
        
        # Extração do ID do tweet da URL
        tweet_id = ""
        if tweet_url:
            match = re.search(r'/status/(\d+)', tweet_url)
            if match:
                tweet_id = match.group(1)
        
        # Construir objeto de tweet
        tweet_data = {
            'username': username,
            'text': tweet_text,
            'timestamp': timestamp,
            'url': tweet_url,
            'id': tweet_id,
            'raw_html': tweet_element.get_attribute('outerHTML')  # Para debug ou processamento posterior
        }
        
        return tweet_data
    
    except StaleElementReferenceException:
        print("Elemento se tornou obsoleto durante a extração")
        return None
    except Exception as e:
        print(f"Erro ao extrair dados do tweet: {e}")
        return None

def search_tweets(driver, query=None, within_profile=None, tweet_count=None, since_date=None, until_date=None, max_scroll=None):
    """
    Função aprimorada para buscar tweets com mais opções de filtragem
    
    Parâmetros:
    - query: Termo de pesquisa
    - within_profile: Username do perfil para buscar dentro
    - tweet_count: Número máximo de tweets a recuperar (None = todos possíveis)
    - since_date: Data de início no formato 'YYYY-MM-DD'
    - until_date: Data final no formato 'YYYY-MM-DD'
    - max_scroll: Número máximo de scrolls (None = continuar até obter tweet_count)
    """
    # Construir URL da consulta
    query_URL = ''
    
    # Adicionar parâmetros de data se fornecidos
    date_params = ""
    if since_date:
        date_params += f" since:{since_date}"
    if until_date:
        date_params += f" until:{until_date}"
    
    # Construir URL baseada nos parâmetros
    if query is None and within_profile is not None:
        query_URL = f"https://x.com/{within_profile}/with_replies"
    elif within_profile and query:
        full_query = f"{query}{date_params}"
        query_URL = f"https://x.com/{within_profile}/search?q={full_query}&f=live"
    else:
        full_query = f"{query}{date_params}"
        query_URL = f"https://x.com/search?q={full_query}&f=live"
    
    print(f"Acessando URL: {query_URL}")
    driver.get(query_URL)
    
    # Aguardar carregamento inicial
    try:
        WebDriverWait(driver, 20).until(
            EC.visibility_of_element_located((By.XPATH, "//article"))
        )
    except TimeoutException:
        print("Nenhum tweet encontrado ou tempo de espera esgotado.")
        return []
    
    tweets = []
    seen_tweet_ids = set()
    scroll_count = 0
    no_new_tweets_count = 0
    initial_tweet_count = 0
    last_height = driver.execute_script("return document.body.scrollHeight")
    
    # Definir limite de scrolls se não especificado
    if max_scroll is None:
        max_scroll = 1000  # Um valor alto para continuar até esgotar os tweets
        
    # Loop principal de coleta
    while (tweet_count is None or len(tweets) < tweet_count) and scroll_count < max_scroll and no_new_tweets_count < 5:
        # Coletar tweets visíveis
        tweet_elements = driver.find_elements(By.XPATH, "//article")
        tweets_before = len(tweets)
        
        # Na primeira iteração, registre o número inicial de tweets
        if scroll_count == 0:
            initial_tweet_count = len(tweet_elements)
            print(f"Tweets inicialmente carregados: {initial_tweet_count}")
        
        # Processar tweets visíveis
        for tweet in tweet_elements:
            try:
                # Tentar extrair ID do tweet para evitar duplicações
                tweet_data = extract_tweet_data(tweet)
                
                if tweet_data and tweet_data['id'] and tweet_data['id'] not in seen_tweet_ids:
                    seen_tweet_ids.add(tweet_data['id'])
                    tweets.append(tweet_data)
                    
                    # Informar progresso
                    if len(tweets) % 10 == 0:
                        print(f"Tweets coletados: {len(tweets)}")
                        
                    # Verificar se atingimos o limite
                    if tweet_count and len(tweets) >= tweet_count:
                        print(f"Limite de {tweet_count} tweets atingido!")
                        break
            except StaleElementReferenceException:
                continue
        
        # Verificar se novos tweets foram encontrados
        if len(tweets) == tweets_before:
            no_new_tweets_count += 1
            print(f"Nenhum tweet novo encontrado. Tentativa {no_new_tweets_count}/5")
        else:
            no_new_tweets_count = 0  # Resetar contador se encontramos novos tweets
        
        # Realizar scroll
        scroll_count += 1
        print(f"Realizando scroll {scroll_count}/{max_scroll}")
        
        # Scroll com velocidade variável para evitar detecção
        scroll_height = random.randint(800, 1000)
        driver.execute_script(f"window.scrollBy(0, {scroll_height});")
        
        # Esperar carregamento com tempo variável
        time.sleep(random.uniform(2, 4))
        
        # Verificar se chegamos ao fim da página
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            no_new_tweets_count += 1
        last_height = new_height
        
        # Adicionar pausa aleatória ocasional para simular comportamento humano
        if scroll_count % 5 == 0:
            pause_time = random.uniform(2, 5)
            print(f"Pausa de {pause_time:.1f} segundos...")
            time.sleep(pause_time)
    
    print(f"\nColeta finalizada! Total de tweets: {len(tweets)}")
    print(f"Scrolls realizados: {scroll_count}")
    
    return tweets

def save_tweets_to_file(tweets, filename=None):
    """Salva os tweets coletados em um arquivo"""
    if filename is None:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"tweets_{timestamp}.txt"
    
    with open(filename, "w", encoding="utf-8") as file:
        for i, tweet in enumerate(tweets, 1):
            file.write(f"Tweet #{i}\n")
            file.write(f"Username: {tweet.get('username', 'N/A')}\n")
            file.write(f"Timestamp: {tweet.get('timestamp', 'N/A')}\n")
            file.write(f"URL: {tweet.get('url', 'N/A')}\n")
            file.write(f"Text: {tweet.get('text', 'N/A')}\n")
            file.write("=" * 80 + "\n\n")
    
    print(f"Tweets salvos no arquivo: {filename}")
    return filename

def main():
    # Configuração
    cookie_file = "cookies.pkl"
    driver = setup_driver(headless=True)  # False para debug, True para produção
    
    try:
        driver.get("https://x.com")
        is_logged_in = False
        
        # Tentar usar cookies existentes
        if os.path.exists(cookie_file):
            load_cookies(driver, cookie_file)
            driver.refresh()
            
            # Verificar se login foi mantido
            if "login" not in driver.current_url:
                print("Sessão restaurada dos cookies!")
                is_logged_in = True
        
        # Fazer login se necessário
        if not is_logged_in:
            is_logged_in = login(driver, EMAIL, PASSWORD)
            if is_logged_in:
                save_cookies(driver, cookie_file)
        
        if not is_logged_in:
            print("Não foi possível fazer login. Tentando continuar sem autenticação...")
        
        # Parâmetros de busca (personalize conforme necessário)
        query = None  # Palavra-chave opcional
        profile = "AswathDamodaran"  # Perfil a ser coletado
        
        # Datas para filtrar (opcional)
        since_date = "2023-01-01"  # Desde 1 de janeiro de 2023
        until_date = None  # Até a data atual
        
        # Limite de tweets (remova para coletar todos possíveis)
        num_tweets = 200
        
        # Coletar tweets
        tweets = search_tweets(
            driver,
            query=query,
            within_profile=profile,
            tweet_count=num_tweets,
            since_date=since_date,
            until_date=until_date,
            max_scroll=30  # Limitar número de scrolls
        )
        
        # Salvar resultados
        if tweets:
            save_tweets_to_file(tweets, f"tweets_{profile}_{len(tweets)}.txt")
        else:
            print("Nenhum tweet foi coletado!")

    except Exception as e:
        print(f"Erro durante execução: {e}")
    
    finally:
        print("Encerrando navegador...")
        driver.quit()

if __name__ == "__main__":
    main()