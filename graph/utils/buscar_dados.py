import os
from pathlib import Path

def listar_arquivos_por_pasta(pasta: Path) -> dict[str, list[Path]]:
    """
    Recursively lists all files in the specified path, grouping them by their immediate parent folder.

    Args:
        pasta (Path): Directory path to scan.

    Returns:
        dict[str, list[Path]]: Dictionary with the last folder name as keys and lists of full file paths as values.
    """
    file_dict = {}

    try:
        # Verifica se o caminho existe e é um diretório
        if not pasta.exists():
            print(f"O diretório {pasta} não existe!")
            return file_dict
        if not pasta.is_dir():
            print(f"O caminho {pasta} não é um diretório!")
            return file_dict

        # Itera sobre todos os arquivos recursivamente
        for item in pasta.rglob('*'):
            if item.is_file():
                # Obtém o nome da última pasta (pasta pai imediata)
                parent_folder = item.parent.name or 'root'
                # Inicializa a lista para a pasta, se ainda não existir
                if parent_folder not in file_dict:
                    file_dict[parent_folder] = []
                # Adiciona o caminho completo do arquivo
                file_dict[parent_folder].append(item)

        return file_dict

    except PermissionError:
        print(f"Erro: Permissão negada para acessar {pasta}.")
        return file_dict
    except OSError as e:
        print(f"Erro: Falha ao acessar {pasta}: {e}")
        return file_dict
    except Exception as e:
        print(f"Erro inesperado ao listar arquivos: {e}")
        return file_dict