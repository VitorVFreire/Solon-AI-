import sqlite3
import os
import time
from tqdm import tqdm

BATCH_SIZE = 1000  # Número de comandos antes do commit

conn = sqlite3.connect("cnpj.db")
cursor = conn.cursor()

# Otimizações do SQLite para escrita em massa
cursor.execute("PRAGMA synchronous = OFF;")
cursor.execute("PRAGMA journal_mode = MEMORY;")

# Criação das tabelas
cursor.execute('''
    CREATE TABLE IF NOT EXISTS cnaes (
        codigo_atividade INTEGER PRIMARY KEY,
        nome_atividade TEXT NOT NULL
    );
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS empresas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        numero_inscricao TEXT NOT NULL,
        razao_social TEXT NOT NULL,
        codigo_atividade INTEGER NOT NULL,
        codigo_municipio TEXT NOT NULL,
        capital_social TEXT NOT NULL,
        natureza_juridica TEXT NOT NULL,
        coluna_vazia_1 TEXT,
        FOREIGN KEY(codigo_atividade) REFERENCES cnaes(codigo_atividade)
    );
''')

conn.commit()

# Caminho para os arquivos .sql
path = 'sql'
arquivos_sql = [f for f in os.listdir(path) if f.lower().endswith('.sql')]
caminhos_arquivos = [os.path.join(path, arquivo) for arquivo in arquivos_sql]

for caminho in caminhos_arquivos:
    print(f"Processando {os.path.basename(caminho)}")
    with open(caminho, 'r', encoding='utf-8') as f:
        buffer = ""
        count = 0

        for linha in tqdm(f, desc=f"Lendo {os.path.basename(caminho)}", unit=" linhas"):
            linha = linha.strip()
            if not linha:
                continue
            buffer += " " + linha
            if ";" in linha:
                comando = buffer.strip()
                buffer = ""
                try:
                    cursor.execute(comando)
                except sqlite3.IntegrityError:
                    pass  # Pode ignorar duplicatas ou erros de integridade
                except Exception as e:
                    print(f"Erro ao executar comando:\n{comando}\n{e}")
                count += 1
                if count >= BATCH_SIZE:
                    conn.commit()
                    count = 0

        # Commit final
        if count > 0:
            conn.commit()

# Verifica os dados inseridos
cursor.execute("SELECT COUNT(*) FROM cnaes")
print(f"Total de CNAEs inseridos: {cursor.fetchone()[0]}")

conn.close()
