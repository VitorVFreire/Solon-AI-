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

@app.get("/candlestick/{setor}")
def get_candlestick(setor: str, perfil: Optional[str] = Query(None)):
    add_query = "ea.name = $setor" if perfil is None else "ea.name = $setor AND a.profile = $perfil"
    
    query = f"""
    MATCH (ea:EconomicActivity)<-[:ANALYZES_ACTIVITY {{activity_type: 'Sector Focus'}}]-(a:Analysis)<-[:HAS_ANALYSIS]-(n:News)
    WHERE n.published_at IS NOT NULL AND {add_query}
    WITH ea, a.profile AS profile_name, 
         COALESCE(a.sector_score, a.personal_score, 0) AS score,
         datetime(n.published_at) AS pub_date,
         duration.inDays(datetime(n.published_at), datetime()).days AS days_diff
    WITH ea, profile_name, date(pub_date) AS dia,
         score,
         CASE 
             WHEN days_diff IS NULL OR days_diff < 0 THEN 1.0
             ELSE exp(-toFloat(days_diff) / 30.0) 
         END AS weight
    WITH ea, profile_name, dia, SUM(score * weight) AS weighted_sum, SUM(weight) AS total_weight
    WITH ea, profile_name, dia, weighted_sum / total_weight AS daily_score
    RETURN ea.name AS setor, profile_name AS perfil, dia, daily_score
    ORDER BY dia
    """

    params = {"setor": setor}
    if perfil:
        params["perfil"] = perfil

    with driver.session() as session:
        result = session.run(query, **params)
        data = [record.data() for record in result]

    for row in data:
        if hasattr(row["dia"], "to_native"):
            row["dia"] = row["dia"].to_native().isoformat()

    return {"data": data}

@app.get("/daily/{setor}")
def get_daily_scores(setor: str):
    query = """
    MATCH (ea:EconomicActivity)<-[:ANALYZES_ACTIVITY {activity_type: 'Sector Focus'}]-(a:Analysis)<-[:HAS_ANALYSIS]-(n:News)
    WHERE n.published_at IS NOT NULL AND ea.name = $setor
    WITH ea.name AS setor,
         date(datetime(n.published_at)) AS dia,
         COALESCE(a.sector_score, a.personal_score, 0) AS score,
         duration.inDays(datetime(n.published_at), datetime()).days AS days_diff
    WITH setor, dia, score,
         CASE
            WHEN days_diff IS NULL OR days_diff < 0 THEN 1.0
            ELSE exp(-toFloat(days_diff) / 30.0)
         END AS weight
    WITH setor, dia, SUM(score * weight) / SUM(weight) AS score_diario
    RETURN setor, dia, score_diario
    ORDER BY dia
    """
    with driver.session() as session:
        result = session.run(query, setor=setor)
        rows = [record.data() for record in result]

    if not rows:
        raise HTTPException(status_code=404, detail="Setor não encontrado")

    for row in rows:
        if hasattr(row["dia"], "to_native"):
            row["dia"] = row["dia"].to_native()

    start_date = rows[0]["dia"]
    end_date = rows[-1]["dia"]
    current_date = start_date
    idx = 0
    last_score = rows[0]["score_diario"]

    filled = []
    while current_date <= end_date:
        if idx < len(rows) and rows[idx]["dia"] == current_date:
            last_score = rows[idx]["score_diario"]
            filled.append({"dia": str(current_date), "score": last_score})
            idx += 1
        else:
            filled.append({"dia": str(current_date), "score": last_score})
        current_date += timedelta(days=1)

    return {"setor": setor, "data": filled}

