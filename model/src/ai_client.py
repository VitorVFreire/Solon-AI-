import os
import requests
from typing import Dict, Any
from dotenv import load_dotenv
from typing import Dict, Literal

# Carrega variáveis de ambiente
load_dotenv()

# Configuração da API
def setup_client(api_service: Literal['openAI', 'grok'] = 'openAI') -> Dict[str, str]:
    if api_service == 'openAI':
        api_key = os.getenv("OPENAI_API_KEY")
        base_url = 'https://api.openai.com/v1'
        model = 'gpt-3.5-turbo'
    elif api_service == 'grok':
        api_key = os.getenv("XAI_API_KEY")
        base_url = 'https://api.x.ai/v1'
        model = 'grok-3'
    else:
        raise ValueError("Sem Definição de Serviço!")
    
    if not api_key:
        raise ValueError("API_KEY não encontrada nas variáveis de ambiente")
    return {
        "api_key": api_key,
        "base_url": base_url,
        "model": model
    }

# Classe para interação com a API da OpenAI
class AIClient:
    def __init__(self, config: Dict[str, str]):
        self.api_key = config["api_key"]
        self.base_url = config["base_url"]
        self.model = config["model"]
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
    
    def invoke(self, input_data: Dict[str, Any]) -> str:
        if not input_data.get("system") or not isinstance(input_data["system"], str):
            raise ValueError("System prompt is missing or invalid")
        if not input_data.get("human") or not isinstance(input_data["human"], str):
            raise ValueError("Human prompt is missing or invalid")
        
        max_tokens = int(os.getenv("MAX_TOKENS", "4096"))
        temperature = float(os.getenv("TEMPERATURE", "0.2"))
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": input_data["system"]},
                {"role": "user", "content": input_data["human"]}
            ],
            "max_tokens": max_tokens,
            "temperature": temperature
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/chat/completions",
                headers=self.headers,
                json=payload
            )
            response.raise_for_status()
            response_data = response.json()
            if not response_data.get("choices") or not response_data["choices"][0].get("message"):
                raise ValueError("Resposta da API não contém 'choices' ou 'message'")
            return response_data["choices"][0]["message"]["content"]
        except requests.exceptions.HTTPError as e:
            print(f"Erro HTTP na API: {e}")
            print(f"Resposta da API: {response.text}")
            raise Exception(f"Erro ao chamar a API da OpenAI: {str(e)}")
        except Exception as e:
            print(f"Erro inesperado: {e}")
            raise