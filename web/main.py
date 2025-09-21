# backend.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

app = FastAPI()

# Habilita CORS para frontend
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
def get_candlestick(setor: str):
    query = """
    MATCH (ea:EconomicActivity)<-[:ANALYZES_ACTIVITY {activity_type: 'Sector Focus'}]-(a:Analysis)<-[:HAS_ANALYSIS]-(n:News)
    WHERE n.published_at IS NOT NULL AND ea.name = $setor
    WITH ea, a.profile AS profile_name, 
         COALESCE(a.sector_score, a.personal_score, 0) AS score,
         datetime(n.published_at) AS pub_date,
         duration.inDays(datetime(n.published_at), datetime()).days AS days_diff,
         CASE
            WHEN duration.inDays(datetime(n.published_at), datetime()).days IS NULL OR duration.inDays(datetime(n.published_at), datetime()).days < 0 THEN 1.0
            ELSE exp(-toFloat(duration.inDays(datetime(n.published_at), datetime()).days) / 30.0)
         END AS weight,
         n
    WITH ea, profile_name, SUM(score * weight)/SUM(weight) AS weighted_score, 
         COLLECT({
            news_url: n.url,
            published_at: n.published_at,
            score: score,
            weight: weight
         }) AS news_details
    RETURN ea.name AS setor, profile_name AS perfil, weighted_score AS media_ponderada, news_details
    ORDER BY perfil
    """
    with driver.session() as session:
        result = session.run(query, setor=setor)
        data = [record.data() for record in result]
    if not data:
        raise HTTPException(status_code=404, detail="Setor não encontrado")
    return {"data": data}

