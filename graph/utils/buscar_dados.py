import os
from pathlib import Path

def listar_arquivos_por_pasta(pasta: Path) -> dict[str, list[Path]]:
    file_dict = {}

    try:
        if not pasta.exists():
            print(f"O diretório {pasta} não existe!")
            return file_dict
        if not pasta.is_dir():
            print(f"O caminho {pasta} não é um diretório!")
            return file_dict

        for item in pasta.rglob('*'):
            if item.is_file():
                parent_folder = item.parent.name or 'root'
                if parent_folder not in file_dict:
                    file_dict[parent_folder] = []
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