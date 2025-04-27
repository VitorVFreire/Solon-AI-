import os
import pandas as pd
from typing import Tuple, List, Dict, Any

def load_activities(json_path: str) -> Tuple[List[str], List[Dict[str, Any]]]:
    """
    Carrega atividades econômicas de um arquivo JSON.

    Args:
        json_path (str): Caminho do arquivo JSON contendo as atividades econômicas.
                        Padrão: database/economic_activities.json

    Returns:
        Tuple[List[str], pd.Series]: Uma tupla contendo:
            - Lista de nomes das atividades econômicas (atividade_economica).
            - Série pandas com os dados completos das atividades (dicionários).

    Raises:
        FileNotFoundError: Se o arquivo JSON não for encontrado.
        KeyError: Se a chave 'atividades_economicas' não estiver presente no JSON.
        ValueError: Se o formato do JSON for inválido ou as atividades não forem uma lista.
    """
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"Arquivo JSON não encontrado: {json_path}")
    
    try:
        # Carrega o JSON como DataFrame
        df = pd.read_json(json_path, orient='records')
        
        # Verifica se a chave 'atividades_economicas' existe
        if 'atividades_economicas' not in df:
            raise KeyError("Chave 'atividades_economicas' não encontrada no arquivo JSON")
        
        # Extrai a lista de atividades econômicas
        atividades_data = df['atividades_economicas']
        
        # Extrai os nomes das atividades
        atividades_lista = []
        for item in atividades_data:
            if not isinstance(item, dict) or 'atividade_economica' not in item:
                print(f"Aviso: Item inválido encontrado em atividades_economicas: {item}")
                continue
            atividades_lista.append(item['atividade_economica'])
        
        return atividades_lista, atividades_data
    
    except ValueError as e:
        raise ValueError(f"Erro ao parsear o arquivo JSON: {str(e)}")
    except Exception as e:
        raise Exception(f"Erro inesperado ao carregar atividades: {str(e)}")