import os
import json
import pandas as pd
from typing import List, Dict, Any
from tqdm import tqdm
import re
from utils import clean_filename

class ActivitiesGenerate:
    def __init__(self, llm_client: Any, system_prompt_file: str, 
                 human_prompt_file: str, file_name: str, output_dir: str = "resultados"):
        self.llm_client = llm_client
        self.output_dir = output_dir
        self.file_name = file_name
        
        # Valida arquivos de prompt
        if not os.path.exists(system_prompt_file):
            raise FileNotFoundError(f"Arquivo de system prompt não encontrado: {system_prompt_file}")
        if not os.path.exists(human_prompt_file):
            raise FileNotFoundError(f"Arquivo de human prompt não encontrado: {human_prompt_file}")
        
        self.system_prompt = open(system_prompt_file, encoding='utf-8').read()
        self.human_prompt = open(human_prompt_file, encoding='utf-8').read()
        os.makedirs(self.output_dir, exist_ok=True)

    def _clean_markdown(self, text: str) -> str:
        """Remove Markdown code block delimiters and extract JSON content."""
        pattern = r'```json\s*([\s\S]*?)\s*```'
        match = re.search(pattern, text)
        if match:
            return match.group(1).strip()
        return text.strip()

    def generate_dependencies(self, state: Dict[str, Any]) -> Dict[str, Any]:
        batch_size = state.get('batch_size', 20)
        amount_activities = state['amount_activities']
        results = []
        required_keys = {"atividade_economica", "descricao_atividade", "tipo_atividade", "nivel_de_importancia"}
        generated_activities = state.get('generated_activities', set())

        # Calcula o número de lotes
        num_batches = (amount_activities + batch_size - 1) // batch_size

        for i in tqdm(range(num_batches), desc="Processando lotes", unit="lote"):
            current_batch_size = min(batch_size, amount_activities - i * batch_size)
            tqdm.write(f"Gerando lote {i+1}/{num_batches} com {current_batch_size} atividades")
            
            activities_context = ", ".join(generated_activities) if generated_activities else "Nenhuma atividade gerada ainda"
            input_data = {
                "system": self.system_prompt,
                "human": self.human_prompt.format(
                    amount_activities=current_batch_size,
                    existing_activities=activities_context
                )
            }
            
            try:
                result = self.llm_client.invoke(input_data)
                cleaned_result = self._clean_markdown(result)
                json_result = json.loads(cleaned_result)
                
                if isinstance(json_result, list):
                    for item in json_result:
                        if not isinstance(item, dict) or not all(key in item for key in required_keys):
                            results.append({
                                "error": f"Item inválido no lote {i+1}: chaves ausentes ou formato incorreto",
                                "raw_response": item
                            })
                            continue
                        
                        activity_name = item["atividade_economica"].lower()
                        if activity_name in generated_activities:
                            results.append({
                                "error": f"Atividade duplicada no lote {i+1}: {item['atividade_economica']}",
                                "raw_response": item
                            })
                            tqdm.write(f"Aviso: Atividade duplicada '{item['atividade_economica']}' ignorada no lote {i+1}")
                            continue
                        
                        results.append(item)
                        generated_activities.add(activity_name)
                
                else:
                    if not all(key in json_result for key in required_keys):
                        results.append({
                            "error": f"Resposta inválida no lote {i+1}: chaves ausentes",
                            "raw_response": json_result
                        })
                    else:
                        activity_name = json_result["atividade_economica"].lower()
                        if activity_name in generated_activities:
                            results.append({
                                "error": f"Atividade duplicada no lote {i+1}: {json_result['atividade_economica']}",
                                "raw_response": json_result
                            })
                            tqdm.write(f"Aviso: Atividade duplicada '{json_result['atividade_economica']}' ignorada no lote {i+1}")
                        else:
                            results.append(json_result)
                            generated_activities.add(activity_name)
                
                valid_items = [item for item in results[-current_batch_size:] if "error" not in item]
                if len(valid_items) < current_batch_size:
                    tqdm.write(f"Aviso: Lote {i+1} retornou {len(valid_items)} atividades válidas de {current_batch_size} solicitadas")
            
            except json.JSONDecodeError:
                results.append({
                    "error": f"Não foi possível parsear a resposta como JSON no lote {i+1}",
                    "raw_response": result
                })
                tqdm.write(f"Erro: Não foi possível parsear JSON no lote {i+1}")
            except Exception as e:
                results.append({
                    "error": f"Erro ao processar lote {i+1}: {str(e)}",
                    "raw_response": None
                })
                tqdm.write(f"Erro: Falha no lote {i+1}: {str(e)}")

        state["result"] = results
        state["generated_activities"] = generated_activities
        return state
    
    def format_output(self, state: Dict[str, Any]) -> Dict[str, Any]:
        results = state["result"]
        amount_activities = state["amount_activities"]
        
        # Remove duplicatas e filtra erros
        seen_activities = set()
        unique_activities = []
        duplicates_found = 0
        errors = []
        
        for item in results:
            if "error" in item:
                errors.append(item)
                continue
            activity_name = item["atividade_economica"].lower()
            if activity_name not in seen_activities:
                seen_activities.add(activity_name)
                unique_activities.append(item)
            else:
                duplicates_found += 1
                print(f"Aviso: Atividade duplicada '{item['atividade_economica']}' removida ao gerar JSON")
        
        # Conta atividades válidas
        if len(unique_activities) < amount_activities:
            print(f"Aviso: Geradas {len(unique_activities)} atividades válidas de {amount_activities} solicitadas")
        if duplicates_found > 0:
            print(f"Aviso: {duplicates_found} atividades duplicadas foram removidas do JSON final")
        
        # Estrutura o JSON com a chave atividades_economicas
        output_json = {
            "atividades_economicas": unique_activities
        }
        # Inclui erros, se houver, em uma chave separada (opcional)
        if errors:
            output_json["erros"] = errors
            print(f"Aviso: {len(errors)} erros registrados no JSON final")
        
        # Gera o arquivo JSON
        output_path = os.path.join(self.output_dir, f"{self.file_name}.json")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_json, f, ensure_ascii=False, indent=2)
        
        # Converte apenas as atividades válidas para um DataFrame
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
            "generated_activities": set()
        }
        
        state = self.generate_dependencies(state)
        state = self.format_output(state)
        return state