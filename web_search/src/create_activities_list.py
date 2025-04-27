import os
import re
import json
import pandas as pd
from typing import List, Dict, Any, Tuple
from dotenv import load_dotenv
from tqdm import tqdm  # Para mostrar uma barra de progresso

# Carrega variáveis de ambiente
load_dotenv()

# Classe para processar as atividades econômicas
class CreateActivities:
    def __init__(self, llm_client, system_prompt_file: str, human_prompt_file: str, name_file: str, output_dir: str = "database"):
        self.llm_client = llm_client
        self.output_dir = output_dir
        self.name_file = name_file
        
        if not os.path.exists(system_prompt_file):
            raise FileNotFoundError(f"Arquivo de system prompt não encontrado: {system_prompt_file}")
        if not os.path.exists(human_prompt_file):
            raise FileNotFoundError(f"Arquivo de human prompt não encontrado: {human_prompt_file}")
            
        with open(system_prompt_file, encoding='utf-8') as f:
            self.system_prompt = f.read()
        with open(human_prompt_file, encoding='utf-8') as f:
            self.human_prompt = f.read()
        os.makedirs(self.output_dir, exist_ok=True)

    def extract_json(self, raw_response: str) -> str:
        """Extrai o bloco JSON de uma resposta."""
        # Primeiro tenta encontrar entre blocos markdown
        json_match = re.search(r'```json\n(.*?)\n```', raw_response, re.DOTALL)
        if json_match:
            return json_match.group(1).strip()
        
        # Se não encontrou, tenta encontrar o primeiro '{' e extrair o JSON manualmente
        try:
            # Tenta processar como um array de objetos JSON
            if raw_response.strip().startswith('[') and raw_response.strip().endswith(']'):
                return raw_response.strip()
                
            # Tenta encontrar o primeiro '{' e extrair o JSON 
            first_brace = raw_response.find('{')
            last_brace = raw_response.rfind('}')
            if first_brace != -1 and last_brace != -1:
                return raw_response[first_brace:last_brace+1].strip()
        except:
            pass
        
        return raw_response.strip()

    def generate_activities_batch(self, quantidade_atividades: int) -> Tuple[List[Dict[str, Any]], bool]:
        """Gera um lote de atividades econômicas e retorna se houve sucesso."""
        input_data = {
            "system": self.system_prompt,
            "human": self.human_prompt.format(quantidade_atividades=quantidade_atividades)
        }
        
        try:
            raw_response = self.llm_client.invoke(input_data)
            json_str = self.extract_json(raw_response)
            
            # Tenta interpretar o resultado como JSON
            try:
                json_result = json.loads(json_str)
                
                # Verificar se o JSON contém a chave "atividades_economicas"
                if isinstance(json_result, dict) and "atividades_economicas" in json_result:
                    json_result = json_result["atividades_economicas"]
                elif not isinstance(json_result, list):
                    json_result = [json_result]
                    
                validated_result = []
                for activity in json_result:
                    if not isinstance(activity, dict):
                        continue
                    
                    # Corrigindo o nome da chave de nivel_importancia para corresponder ao sistema
                    nivel_importancia = activity.get("nivel_importancia", 
                                        activity.get("nivel_de_importancia", 0))
                    
                    # Normaliza o nivel_importancia para ser numérico
                    if isinstance(nivel_importancia, str):
                        try:
                            nivel_importancia = int(nivel_importancia)
                        except:
                            nivel_importancia = 0
                    
                    validated_activity = {
                        "atividade_economica": activity.get("atividade_economica", "Desconhecida"),
                        "descricao_atividade": activity.get("descricao_atividade", "Sem descrição"),
                        "tipo_atividade": activity.get("tipo_atividade", "Desconhecido"),
                        "nivel_importancia": nivel_importancia
                    }
                    validated_result.append(validated_activity)
                
                print(f"Gerado {len(validated_result)} atividades neste lote")
                return validated_result, True
                
            except json.JSONDecodeError as e:
                print(f"Erro ao parsear JSON: {e}")
                print(f"Resposta bruta: {raw_response[:100]}...")  # Mostra parte da resposta
                return [], False
                
        except Exception as e:
            print(f"Erro ao invocar a API: {str(e)}")
            return [], False

    def process_activities(self, quantidade_atividades: int, batch_size: int = 10) -> Dict[str, Any]:
        """Processa atividades em blocos e acumula em uma única lista, evitando duplicatas."""
        if not isinstance(quantidade_atividades, int) or quantidade_atividades <= 0:
            raise ValueError("quantidade_atividades deve ser um número inteiro positivo")
        
        all_activities = []
        seen_activities = set()
        
        # Criar uma lista temporária para salvar progresso parcial
        temp_file = os.path.join(self.output_dir, f"{self.name_file}_temp.json")
        
        # Verificar se há arquivo temporário existente para continuar processamento
        if os.path.exists(temp_file):
            try:
                with open(temp_file, 'r', encoding='utf-8') as f:
                    temp_data = json.load(f)
                    temp_activities = temp_data.get("atividades_economicas", [])
                    
                    for activity in temp_activities:
                        name = activity["atividade_economica"]
                        if name and name not in seen_activities and name != "Desconhecida":
                            seen_activities.add(name)
                            all_activities.append(activity)
                    
                    print(f"Carregado {len(all_activities)} atividades do arquivo temporário")
            except Exception as e:
                print(f"Erro ao carregar arquivo temporário: {e}")
        
        # Configuração para controle de erros
        MAX_CONSECUTIVE_ERRORS = 3
        MAX_TOTAL_ERRORS = 10
        consecutive_errors = 0
        total_errors = 0
        
        # Barra de progresso
        with tqdm(total=quantidade_atividades, initial=len(all_activities)) as pbar:
            # Loop enquanto não atingirmos a quantidade desejada
            while len(all_activities) < quantidade_atividades and total_errors < MAX_TOTAL_ERRORS:
                # Calcular quanto falta gerar
                remaining = quantidade_atividades - len(all_activities)
                current_batch_size = min(batch_size, remaining)
                
                # Se não faltam atividades, sair do loop
                if remaining <= 0:
                    break
                
                # Tenta gerar atividades
                activities, success = self.generate_activities_batch(current_batch_size)
                
                if success:
                    consecutive_errors = 0  # Resetar erro se sucesso
                    new_added = 0
                    
                    # Adicionar apenas atividades únicas
                    for activity in activities:
                        # Se já atingiu o total, não adicionar mais
                        if len(all_activities) >= quantidade_atividades:
                            break
                            
                        name = activity["atividade_economica"]
                        if name and name not in seen_activities and name != "Desconhecida":
                            seen_activities.add(name)
                            all_activities.append(activity)
                            new_added += 1
                    
                    # Atualizar barra de progresso (apenas para o que foi adicionado)
                    pbar.update(new_added)
                    
                    # Salvar progresso parcial
                    temp_output = {
                        "atividades_economicas": all_activities[:quantidade_atividades]
                    }
                    with open(temp_file, 'w', encoding='utf-8') as f:
                        json.dump(temp_output, f, ensure_ascii=False, indent=2)
                        
                else:
                    consecutive_errors += 1
                    total_errors += 1
                    print(f"Erro ao gerar atividades (erro {consecutive_errors}/{MAX_CONSECUTIVE_ERRORS}, total: {total_errors}/{MAX_TOTAL_ERRORS})")
                    
                    # Se muitos erros consecutivos, diminui tamanho do lote
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS and batch_size > 2:
                        batch_size = max(2, batch_size // 2)
                        print(f"Reduzindo tamanho do lote para {batch_size}")
                        consecutive_errors = 0
        
        # Verificar se conseguimos gerar a quantidade solicitada
        if len(all_activities) < quantidade_atividades:
            print(f"ATENÇÃO: Gerado apenas {len(all_activities)} de {quantidade_atividades} atividades solicitadas")
        elif len(all_activities) > quantidade_atividades:
            print(f"ATENÇÃO: Limitando de {len(all_activities)} para {quantidade_atividades} atividades solicitadas")
            all_activities = all_activities[:quantidade_atividades]
        
        # Salvar resultado final (exatamente a quantidade solicitada)
        output_data = {
            "atividades_economicas": all_activities[:quantidade_atividades]
        }
        
        output_path = os.path.join(self.output_dir, f"{self.name_file}.json")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        # Remover arquivo temporário se existir
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except:
                pass
        
        print(f"Finalizado: {len(all_activities)} atividades salvas em {output_path}")
        
        return {
            "quantidade_atividades": len(all_activities),
            "formatted_result": pd.DataFrame(all_activities)
        }
