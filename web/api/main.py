# backend.py
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
from datetime import date, timedelta
from typing import Optional
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

@app.get("/setores")
def get_setores():
    query = "MATCH (ea:EconomicActivity) RETURN DISTINCT ea.name AS setor ORDER BY ea.name"
    with driver.session() as session:
        result = session.run(query)
        setores = [record["setor"] for record in result]
    return {"setores": setores}

@app.get("/companies")
def get_empresas():
    query = "MATCH (c:Company) RETURN DISTINCT c.company_name AS companies ORDER BY c.company_name"
    with driver.session() as session:
        result = session.run(query)
        companies = [record["companies"] for record in result]
    return {"companies": companies}

@app.get("/{setor}")
def get_dados_setores(
    setor: str, 
    perfil: Optional[str] = Query(None),
    tipo: str = Query("Diario", regex="^(Diario|Mensal)$")
):
    add_query = "ea.name = $setor" if perfil is None else "ea.name = $setor AND a.profile = $perfil"

    if tipo == "Mensal":
        date_group = "date.truncate('month', pub_date)"
        score_alias = "monthly_score"
    else:
        date_group = "date(pub_date)"
        score_alias = "daily_score"

    query = f"""
    MATCH (ea:EconomicActivity)<-[:ANALYZES_ACTIVITY {{activity_type: 'Sector Focus'}}]-(a:Analysis)<-[:HAS_ANALYSIS]-(n:News)
    WHERE n.published_at IS NOT NULL AND {add_query}
    WITH ea, a.profile AS profile_name, 
         COALESCE(a.sector_score, a.personal_score, 0) AS score,
         datetime(n.published_at) AS pub_date,
         duration.inDays(datetime(n.published_at), datetime()).days AS days_diff
    WITH ea, profile_name, {date_group} AS periodo,
         score,
         CASE 
             WHEN days_diff IS NULL OR days_diff < 0 THEN 1.0
             ELSE exp(-toFloat(days_diff) / 30.0) 
         END AS weight
    WITH ea, profile_name, periodo, SUM(score * weight) AS weighted_sum, SUM(weight) AS total_weight
    WITH ea, profile_name, periodo, weighted_sum / total_weight AS {score_alias}
    RETURN ea.name AS setor, profile_name AS perfil, periodo, {score_alias} AS score
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

@app.get("/company/{company_name}")
def get_dados_company(
    company_name: str, 
    perfil: Optional[str] = Query(None),
    tipo: str = Query("Diario", regex="^(Diario|Mensal)$")
):
    add_query = "c.company_name = $company_name"
    if perfil:
        add_query += " AND a.profile = $perfil"

    if tipo == "Mensal":
        date_group = "date.truncate('month', pub_date)"
        score_alias = "monthly_score"
    else:
        date_group = "date(pub_date)"
        score_alias = "daily_score"

    query = f"""
    MATCH (c:Company)<-[:ANALYZES_ENTITY {{entity_type: 'Company'}}]-(a:Analysis)<-[:HAS_ANALYSIS]-(n:News)
    WHERE n.published_at IS NOT NULL AND {add_query}
    WITH c, a.profile AS profile_name, 
         COALESCE(a.personal_score, a.sector_score, 0) AS score,
         datetime(n.published_at) AS pub_date,
         duration.inDays(datetime(n.published_at), datetime()).days AS days_diff
    WITH c, profile_name, {date_group} AS periodo,
         score,
         CASE 
             WHEN days_diff IS NULL OR days_diff < 0 THEN 1.0
             ELSE exp(-toFloat(days_diff) / 30.0) 
         END AS weight
    WITH c, profile_name, periodo, SUM(score * weight) AS weighted_sum, SUM(weight) AS total_weight
    WITH c, profile_name, periodo, weighted_sum / total_weight AS {score_alias}
    RETURN c.company_name AS empresa, profile_name AS perfil, periodo, {score_alias} AS score
    ORDER BY periodo
    """
    
    params = {"company_name": company_name}
    if perfil:
        params["perfil"] = perfil

    with driver.session() as session:
        result = session.run(query, **params)
        data = [record.data() for record in result]

    for row in data:
        if hasattr(row["periodo"], "to_native"):
            row["periodo"] = row["periodo"].to_native().isoformat()

    return {"data": data}

