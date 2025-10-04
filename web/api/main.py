# backend.py
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import yfinance as yf
import pandas as pd
import json
from typing import Optional
from datetime import datetime, timedelta, date
load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
class CandlestickData(BaseModel):
    setor: str
    perfil: str
    media_ponderada: float
    news_details: list
    
def get_datas_b3(symbol: str, start_date: str, end_date: str):
    symbol = symbol + ".SA"
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        if start > datetime.now() or end > datetime.now():
            print("Erro: Datas no futuro não são permitidas")
            return None
    except ValueError:
        print("Erro: Formato de data inválido. Use YYYY-MM-DD")
        return None
    data = yf.download(symbol, start=start_date, end=end_date)
    if data.empty:
        print(f"Erro: Nenhum dado encontrado para {symbol}")
        return None
    df = pd.DataFrame(data['Close'])
    df = df.reset_index()
    df.columns = ['periodo', 'close']
    df['close'] = df['close'].round(2)
    return df

@app.get("/b3/{symbol}/{start_date}/{end_date}")
def get_b3_datas(symbol: str, start_date: str, end_date: str):
    datas_b3 = get_datas_b3(symbol, start_date, end_date)
    if datas_b3 is None:
        raise HTTPException(status_code=404, detail="Nenhum dado encontrado ou entrada inválida")
    data = datas_b3.to_dict(orient='records')  # Convert DataFrame to list of dictionaries
    return {"data": data}

@app.get("/setores")
def get_setores():
    query = "MATCH (ea:EconomicActivity)--(a:Analysis) RETURN DISTINCT ea.name AS setor ORDER BY ea.name"
    with driver.session() as session:
        result = session.run(query)
        setores = [record["setor"] for record in result]
    return {"setores": setores}

@app.get("/companies")
def get_empresas():
    query = "MATCH (c:Company)--(a:Analysis) RETURN DISTINCT c.company_name AS companies, c.symbol as symbol ORDER BY c.company_name"
    with driver.session() as session:
        result = session.run(query)
        data = [record.data() for record in result]
    return {"data": data}

@app.get("/news/setor/{setor}&{perfil}")
def get_news_setor(setor:str, perfil:str):
    query = """
    MATCH (n:News)-[:HAS_ANALYSIS]->(a:Analysis)-[:ANALYZES_ACTIVITY]->(e:EconomicActivity)
    WHERE e.name = $setor AND a.profile = $perfil AND n.published_at IS NOT NULL
    RETURN 
    e.name         AS Setor,
    date(datetime(n.published_at)) AS Data,
    n.title        AS Titulo,
    a.profile      AS Perfil,
    a.sector_score AS Score,
    a.justification_personal as Contexto,
    a.justification_sector as Justificativa,
    n.url          AS URL
    """
    params = {"setor": setor, "perfil": perfil}

    with driver.session() as session:
        result = session.run(query, **params)
        data = [record.data() for record in result]

    for row in data:
        if hasattr(row["Data"], "to_native"):
            dt = row["Data"].to_native()
            row["Data"] = dt.strftime("%d/%m/%Y")

    return {"data": data}

@app.get("/news/company/{company}&{perfil}")
def get_news_company(company:str, perfil:str):
    query = """
    MATCH (n:News)-[:HAS_ANALYSIS]->(a:Analysis)-[:ANALYZES_ENTITY]->(c:Company)
    WHERE c.company_name = $company AND a.profile = $perfil AND n.published_at IS NOT NULL
    RETURN 
    c.company_name         AS Empresa,
    date(datetime(n.published_at)) AS Data,
    n.title        AS Titulo,
    a.profile      AS Perfil,
    a.sector_score AS Score,
    a.justification_personal as Contexto,
    a.justification_sector as Justificativa,
    n.url          AS URL
    ORDER BY Data;
    """
    params = {"company": company, "perfil": perfil}

    with driver.session() as session:
        result = session.run(query, **params)
        data = [record.data() for record in result]

    for row in data:
        if hasattr(row["Data"], "to_native"):
            dt = row["Data"].to_native()
            row["Data"] = dt.isoformat()

    return {"data": data}

@app.get("/{setor}")
def get_dados_setores(
    setor: str,
    perfil: Optional[str] = Query(None),
    tipo: str = Query("Diario", regex="^(Diario|Mensal)$")
):
    add_query = "ea.name = $setor"
    if perfil:
        add_query += " AND a.profile = $perfil"

    if tipo == "Mensal":
        date_group = "date.truncate('month', datetime(n.published_at))"
    else:
        date_group = "date(datetime(n.published_at))"

    query = f"""
    MATCH (ea:EconomicActivity)<-[:ANALYZES_ACTIVITY {{activity_type: 'Sector Focus'}}]-(a:Analysis)<-[:HAS_ANALYSIS]-(n:News)
    WHERE n.published_at IS NOT NULL AND {add_query}
    
    // Agrupa os resultados por setor, perfil e o período (dia ou mês)
    // e calcula a média do score para cada grupo.
    WITH ea, a.profile AS profile_name, {date_group} AS periodo,
         COALESCE(a.sector_score, a.personal_score, 0) AS score

    RETURN ea.name AS setor,
           profile_name AS perfil,
           periodo,
           round(avg(score), 2) AS score  // Usa a função de agregação avg()
    ORDER BY periodo
    """

    params = {"setor": setor}
    if perfil:
        params["perfil"] = perfil

    with driver.session() as session:
        result = session.run(query, **params)
        data = [record.data() for record in result]

    for row in data:
        if hasattr(row["periodo"], "to_native"):
            row["periodo"] = row["periodo"].to_native().isoformat()

    return {"data": data}

from neo4j.time import Date as NeoDate

@app.get("/company/{company_name}")
def get_dados_company(
    company_name: str,
    perfil: Optional[str] = Query(None),
    tipo: str = Query("Diario", regex="^(Diario|Mensal)$")
):
    if tipo == "Mensal":
        date_group = "date.truncate('month', datetime(n.published_at))"
        freq = 'MS'
    else:
        date_group = "date(datetime(n.published_at))"
        freq = 'D'

    query = f"""
    MATCH (c:Company)<-[:ANALYZES_ENTITY {{entity_type: 'Company'}}]-(a:Analysis)<-[:HAS_ANALYSIS]-(n:News)
    WHERE n.published_at IS NOT NULL
      AND c.company_name = $company_name
      AND ($perfil IS NULL OR a.profile = $perfil)
    
    WITH c, a.profile AS profile_name, {date_group} AS periodo,
         COALESCE(a.personal_score, a.sector_score, 0) AS score

    RETURN c.company_name AS company,
           profile_name AS perfil,
           periodo,
           round(avg(score), 2) AS score
    ORDER BY periodo
    """

    params = {"company_name": company_name, "perfil": perfil}

    with driver.session() as session:
        result = session.run(query, **params)
        data = [record.data() for record in result]

    for row in data:
        periodo = row["periodo"]
        if isinstance(periodo, NeoDate):
            row["periodo"] = periodo.to_native()
        elif isinstance(periodo, datetime):
            row["periodo"] = periodo.date()

    if data:
        df = pd.DataFrame(data)
        df['periodo'] = pd.to_datetime(df['periodo'])
        df = df.set_index('periodo')

        full_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq=freq)
        df = df.reindex(full_index)

        df['company'] = df['company'].ffill()
        df['perfil'] = df['perfil'].ffill()
        df['score'] = df['score'].ffill()

        df = df.reset_index().rename(columns={'index': 'periodo'})
        df['periodo'] = df['periodo'].dt.date

        data = df.to_dict(orient='records')

    return {"data": data}