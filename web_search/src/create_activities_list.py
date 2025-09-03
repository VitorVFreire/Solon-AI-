import os
import json
import pandas as pd
from typing import List, Dict, Any
from tqdm import tqdm
import re
from utils import clean_filename
import difflib

class ActivitiesGenerate:
    def __init__(self, llm_client: Any, system_prompt_file: str, 
                 human_prompt_file: str, name_file: str, output_dir: str = "resultados"):
        self.llm_client = llm_client
        self.output_dir = output_dir
        self.name_file = name_file
        
        if not os.path.exists(system_prompt_file):
            raise FileNotFoundError(f"Arquivo de system prompt não encontrado: {system_prompt_file}")
        if not os.path.exists(human_prompt_file):
            raise FileNotFoundError(f"Arquivo de human prompt não encontrado: {human_prompt_file}")
        
        self.system_prompt = open(system_prompt_file, encoding='utf-8').read()
        self.human_prompt = open(human_prompt_file, encoding='utf-8').read()
        os.makedirs(self.output_dir, exist_ok=True)

        self.activities_history_file = os.path.join(self.output_dir, f"{self.name_file}.json")
        self.load_activities_history()

    def load_activities_history(self):
        if os.path.exists(self.activities_history_file):
            with open(self.activities_history_file, 'r', encoding='utf-8') as f:
                self.generated_activities = set(json.load(f))
        else:
            self.generated_activities = set()

    def save_activities_history(self):
        with open(self.activities_history_file, 'w', encoding='utf-8') as f:
            json.dump(list(self.generated_activities), f, ensure_ascii=False, indent=2)

    def _clean_markdown(self, text: str) -> str:
        pattern = r'```json\s*([\s\S]*?)\s*```'
        match = re.search(pattern, text)
        if match:
            return match.group(1).strip()
        return text.strip()

    def _is_similar_activity(self, activity_name: str, existing_activities: set, threshold: float = 0.9) -> bool:
        activity_name = activity_name.lower()
        for existing in existing_activities:
            similarity = difflib.SequenceMatcher(None, activity_name, existing).ratio()
            if similarity > threshold:
                return True
        return False

    def generate_dependencies(self, state: Dict[str, Any]) -> Dict[str, Any]:
        batch_size = state.get('batch_size', 20)
        amount_activities = state['amount_activities']
        results = []
        required_keys = {"atividade_economica", "descricao_atividade", "tipo_atividade", "nivel_de_importancia"}
        
        num_batches = (amount_activities + batch_size - 1) // batch_size
        total_valid_activities = 0
        
        enhanced_system_prompt = self.system_prompt + "\n\n## ATENÇÃO ESPECIAL\nTodas as atividades econômicas devem ser completamente únicas. Evite criar atividades com nomes semelhantes ou conceitos muito próximos. Verifique cuidadosamente cada item antes de incluí-lo na lista."

        while total_valid_activities < amount_activities:
            current_batch_size = min(batch_size, amount_activities - total_valid_activities)
            if current_batch_size <= 0:
                break
                
            tqdm.write(f"Gerando lote com {current_batch_size} atividades ({total_valid_activities}/{amount_activities} completadas)")
            
            activities_context = ", ".join(self.generated_activities) if self.generated_activities else "Nenhuma atividade gerada ainda"
            input_data = {
                "system": enhanced_system_prompt,
                "human": self.human_prompt.format(
                    amount_activities=current_batch_size,
                    existing_activities=activities_context
                )
            }
            
            try:
                result = self.llm_client.invoke(input_data)
                cleaned_result = self._clean_markdown(result)
                json_result = json.loads(cleaned_result)
                
                valid_items = []
                
                if isinstance(json_result, list):
                    for item in json_result:
                        if not isinstance(item, dict) or not all(key in item for key in required_keys):
                            continue
                        
                        activity_name = item["atividade_economica"].lower()
                        if activity_name in self.generated_activities or self._is_similar_activity(activity_name, self.generated_activities):
                            tqdm.write(f"Ignorando atividade duplicada ou semelhante: '{item['atividade_economica']}'")
                            continue
                        
                        valid_items.append(item)
                        self.generated_activities.add(activity_name)
                else:
                    if all(key in json_result for key in required_keys):
                        activity_name = json_result["atividade_economica"].lower()
                        if activity_name not in self.generated_activities and not self._is_similar_activity(activity_name, self.generated_activities):
                            valid_items.append(json_result)
                            self.generated_activities.add(activity_name)
                
                results.extend(valid_items)
                total_valid_activities += len(valid_items)
                
                tqdm.write(f"Status: {total_valid_activities}/{amount_activities} atividades válidas geradas")
                
                if len(valid_items) == 0:
                    batch_size = min(batch_size * 2, 50)
                    tqdm.write(f"Aumentando tamanho do lote para {batch_size} para obter mais atividades únicas")
                
            except json.JSONDecodeError:
                tqdm.write(f"Erro: Não foi possível parsear JSON na resposta")
            except Exception as e:
                tqdm.write(f"Erro: Falha ao processar lote: {str(e)}")

        self.save_activities_history()
        
        state["result"] = results[:amount_activities]
        state["generated_activities"] = self.generated_activities
        return state
    
    def format_output(self, state: Dict[str, Any]) -> Dict[str, Any]:
        results = state["result"]
        amount_activities = state["amount_activities"]
        required_keys = {"atividade_economica", "descricao_atividade", "tipo_atividade", "nivel_de_importancia"}
        
        seen_activities = set()
        unique_activities = []
        
        for item in results:
            activity_name = item["atividade_economica"].lower()
            if activity_name not in seen_activities and not self._is_similar_activity(activity_name, seen_activities):
                for key in required_keys:
                    if key not in item:
                        item[key] = None
                seen_activities.add(activity_name)
                unique_activities.append(item)
        
        if len(unique_activities) < amount_activities:
            print(f"Aviso: Geradas {len(unique_activities)} atividades válidas de {amount_activities} solicitadas")
        
        output_json = {
            "atividades_economicas": unique_activities
        }
        
        output_path = os.path.join(self.output_dir, f"{self.name_file}.json")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_json, f, ensure_ascii=False, indent=2)
        
        state["formatted_result"] = pd.DataFrame(unique_activities)
        return state
    
    def process_activities(self, amount_activities: int, batch_size: int = 20) -> Dict[str, Any]:
        if amount_activities <= 0:
            raise ValueError("O número de atividades deve ser maior que zero")
        if batch_size <= 0:
            raise ValueError("O tamanho do lote deve ser maior que zero")
            
        state = {
            "amount_activities": amount_activities,
            "batch_size": batch_size,
            "generated_activities": self.generated_activities
        }
        
        state = self.generate_dependencies(state)
        state = self.format_output(state)
        return state