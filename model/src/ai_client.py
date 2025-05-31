import os
import requests
from typing import Dict, Any, Literal
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()

# Configuração da API
def setup_client(api_service: Literal['openAI', 'grok'] = 'openAI') -> Dict[str, str]:
    if api_service == 'openAI':
        api_key = os.getenv("OPENAI_API_KEY")
        base_url = 'https://api.openai.com/v1'
        model = os.getenv("OPENAI_MODEL", 'gpt-3.5-turbo') # Permite override do modelo
    elif api_service == 'grok':
        api_key = os.getenv("XAI_API_KEY")
        base_url = 'https://api.x.ai/v1'
        model = os.getenv("GROK_MODEL", 'grok-3') # Permite override do modelo
    else:
        raise ValueError(f"Serviço de API desconhecido: {api_service}. Defina 'openAI' ou 'grok'.")

    if not api_key:
        raise ValueError(f"API_KEY para {api_service} não encontrada nas variáveis de ambiente.")
    return {
        "api_key": api_key,
        "base_url": base_url,
        "model": model,
        "api_service": api_service # Armazena para referência
    }

class AIClient:
    def __init__(self, config: Dict[str, str]):
        self.api_key = config["api_key"]
        self.base_url = config["base_url"]
        self.model = config["model"]
        self.api_service = config["api_service"]
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        # Cabeçalho específico para Grok, se necessário (exemplo, pode variar)
        if self.api_service == 'grok' and "X-API-Key" not in self.headers :
            # self.headers["X-API-Key"] = self.api_key # Exemplo, verifique a documentação do Grok.
            pass


    def invoke(self, system_prompt: str, human_prompt: str) -> str:
        if not system_prompt or not isinstance(system_prompt, str):
            raise ValueError("System prompt está ausente ou é inválido")
        if not human_prompt or not isinstance(human_prompt, str):
            raise ValueError("Human prompt está ausente ou é inválido")

        max_tokens = int(os.getenv("MAX_TOKENS", "2048"))
        temperature = float(os.getenv("TEMPERATURE", "0.1"))

        endpoint = f"{self.base_url}/chat/completions"

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": human_prompt}
            ],
            "max_tokens": max_tokens,
            "temperature": temperature
        }
        
        if self.api_service == 'openAI': 
             payload["response_format"] = {"type": "json_object"}


        try:
            response = requests.post(endpoint, headers=self.headers, json=payload)
            response.raise_for_status()
            response_data = response.json()

            if not response_data.get("choices") or \
               not isinstance(response_data["choices"], list) or \
               not response_data["choices"][0].get("message") or \
               not isinstance(response_data["choices"][0]["message"], dict) or \
               response_data["choices"][0]["message"].get("content") is None:
                raise ValueError("Resposta da API não contém a estrutura esperada 'choices[0].message.content'")
            
            content = response_data["choices"][0]["message"]["content"]
            return content
        
        except requests.exceptions.HTTPError as e:
            error_message = f"Erro HTTP na API ({self.api_service}): {e.response.status_code if e.response else 'N/A'}"
            try:
                error_details = e.response.json() if e.response else str(e)
                error_message += f" - Detalhes: {error_details}"
            except requests.exceptions.JSONDecodeError:
                error_message += f" - Resposta não JSON: {e.response.text if e.response else 'N/A'}"
            print(error_message) # Mantém para logs do servidor
            raise Exception(error_message) from e
        except Exception as e:
            print(f"Erro inesperado ao chamar API ({self.api_service}): {e}")
            raise