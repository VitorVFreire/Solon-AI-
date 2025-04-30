import requests
import json
import urllib.request

# Configurações da API
API_KEY = "aecdc224a584ceabc90d2940faff7a3b"
#url = f"https://gnews.io/api/v4/search?q=ABCBRASILPN&lang=pt&country=br&max=1&from=2010-10-10T00:00:00Z&to=2020-10-10T23:59:59Z&apikey={API_KEY}"
query = "ABC_BRASIL_PN"
encoded_query = urllib.parse.quote(query)
url = f"https://gnews.io/api/v4/search?q={encoded_query}&lang=pt&country=br&max=1&apikey={API_KEY}"

try:
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read().decode("utf-8"))
        articles = data["articles"]
        if articles:
            print(articles)
        else:
            print("No articles found for the given query.")
except urllib.error.HTTPError as e:
    print(f"HTTP Error: {e.code} - {e.reason}")
except urllib.error.URLError as e:
    print(f"URL Error: {e.reason}")
except Exception as e:
    print(f"General Error: {str(e)}")