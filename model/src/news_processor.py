from __future__ import annotations
import os
import json
import datetime as dt
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Set, Tuple

from src.ai_client import AIClient
from src.neo4j_connection import Neo4jConnection
from utils import clean_filename

logger = logging.getLogger(__name__)

class GraphRunner:
    def __init__(self, steps: Dict[str, Any], edges: Dict[str, str], start: str, end: str):
        self.steps = steps
        self.edges = edges
        self.start = start
        self.end = end

    def run(self, state: dict) -> dict:
        node = self.start
        while True:
            step = self.steps[node]
            state = step(state)
            if node == self.end:
                return state
            node = self.edges[node]
@dataclass
class PipelineState:
    news_item: Dict[str, str]
    news_content: str = ""
    news_hash: int = 0
    identified_companies: List[str] = field(default_factory=list)
    identified_sectors: List[str] = field(default_factory=list)
    graph_context: str = ""
    rag_docs: List[Dict[str, Any]] = field(default_factory=list)
    analysis_by_profile: Dict[str, Any] = field(default_factory=dict)
    output_path: Optional[str] = None


class NewsRAGGraph:
    def __init__(
        self,
        llm_client: AIClient,
        neo4j_conn: Optional[Neo4jConnection],
        prompt_paths: Dict[str, str],
        output_dir: str = "output/analysis",
        profiles: Tuple[str, ...] = ("Conservador", "Moderado", "Agressivo"),
        enable_vector_rag: bool = True,
        top_k_news: int = 8,
        top_k_neighbors: int = 8,
        top_k_docs: int = 10,
    ):
        self.llm = llm_client
        self.neo = neo4j_conn
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.profiles = profiles
        self.enable_vector_rag = enable_vector_rag
        self.top_k_news = top_k_news
        self.top_k_neighbors = top_k_neighbors
        self.top_k_docs = top_k_docs

        self.prompts: Dict[str, str] = {}
        for k, p in prompt_paths.items():
            with open(p, "r", encoding="utf-8") as f:
                self.prompts[k] = f.read()

        self.known_companies = self._load_known_entities("company")
        self.known_sectors = self._load_known_entities("sector")

        self.runner = GraphRunner(
            steps={
                "prepare": self._step_prepare,
                "identify": self._step_identify_entities,
                "retrieve": self._step_retrieve_context,
                "analyze": self._step_analyze_profiles,
                "persist": self._step_persist,
            },
            edges={
                "prepare": "identify",
                "identify": "retrieve",
                "retrieve": "analyze",
                "analyze": "persist",
                "persist": "persist",
            },
            start="prepare",
            end="persist",
        )

    def run(self, news_item: Dict[str, str]) -> Dict[str, Any]:
        state = PipelineState(news_item=news_item)
        final_state = self.runner.run(state.__dict__)
        return final_state

    def _load_known_entities(self, entity_type: str) -> List[Dict[str, Any]]:
        if not self.neo:
            return []
        try:
            if entity_type == "company":
                q = (
                    "MATCH (c:Company) WHERE c.company_name IS NOT NULL "
                    "RETURN elementId(c) AS id, c.company_name AS name, c.sector AS sector_property"
                )
                rows = self.neo.execute_query(q)
                return [
                    {"id": r["id"], "name": r["name"], "sector": r.get("sector_property")}
                    for r in rows if r.get("id") and r.get("name")
                ]
            elif entity_type == "sector":
                q = "MATCH (c:Company) WHERE c.sector IS NOT NULL RETURN DISTINCT c.sector AS name"
                rows = self.neo.execute_query(q)
                return [{"name": r["name"]} for r in rows if r.get("name")]
        except Exception as e:
            logger.exception("Failed to load %s: %s", entity_type, e)
        return []

    def _step_prepare(self, state: dict) -> dict:
        item = state["news_item"]
        article = item.get("article") or ""
        title = item.get("title") or ""
        if not article:
            raise ValueError("News item missing 'article' content")
        content = f"{title}\n\n{article}".strip()
        state["news_content"] = content
        state["news_hash"] = hash(content)
        return state

    def _step_identify_entities(self, state: dict) -> dict:
        # Prepare lists
        companies_list = ", ".join(sorted([c["name"] for c in self.known_companies if c.get("name")]))
        sectors_list = ", ".join(sorted([s["name"] for s in self.known_sectors if s.get("name")]))

        human = self.prompts["human_entity_identification"].format(
            news=state["news_content"],
            known_companies_list_str=companies_list,
            known_sectors_list_str=sectors_list,
        )
        system = self.prompts["system_entity_identification"]
        try:
            resp = self.llm.invoke(system, human)
            data = json.loads(resp)
        except Exception:
            logger.exception("Entity identification failed; defaulting to empty sets")
            data = {"identified_companies": [], "identified_sectors": []}

        # Validate against known sets
        valid_companies = [
            n for n in data.get("identified_companies", [])
            if any(c.get("name", "").lower() == n.lower() for c in self.known_companies)
        ]
        valid_sectors = [
            n for n in data.get("identified_sectors", [])
            if any(s.get("name", "").lower() == n.lower() for s in self.known_sectors)
        ]
        state["identified_companies"] = valid_companies
        state["identified_sectors"] = valid_sectors
        return state

    def _step_retrieve_context(self, state: dict) -> dict:
        if not self.neo:
            state["graph_context"] = ""
            state["rag_docs"] = []
            return state

        companies = state["identified_companies"]
        sectors = state["identified_sectors"]

        news_rows: List[Dict[str, Any]] = []
        try:
            if companies:
                q_comp_news = (
                    "MATCH (c:Company)-[:MENTIONED_IN|:OWNS|:SUPPLIES|:PARTNERS_WITH*0..2]-(x) "
                    "WITH DISTINCT c "
                    "MATCH (c)-[:MENTIONED_IN]->(n:News) "
                    "RETURN c.company_name AS company, n.title AS title, n.url AS url, n.snippet AS snippet, n.published_at AS published_at "
                    "ORDER BY published_at DESC LIMIT $k"
                )
                for name in companies:
                    rows = self.neo.execute_query(q_comp_news, {"k": self.top_k_news, "company": name})
                    news_rows.extend(rows)

            if sectors:
                q_sec_news = (
                    "MATCH (ea:EconomicActivity {name: $sector})<-[:ANALYZES_ACTIVITY]-(:Analysis)<-[:HAS_ANALYSIS]-(n:News) "
                    "RETURN $sector AS sector, n.title AS title, n.url AS url, n.snippet AS snippet, n.published_at AS published_at "
                    "ORDER BY published_at DESC LIMIT $k"
                )
                for s in sectors:
                    rows = self.neo.execute_query(q_sec_news, {"sector": s, "k": self.top_k_news})
                    news_rows.extend(rows)
        except Exception:
            logger.exception("Neo4j news retrieval failed")

        rag_docs: List[Dict[str, Any]] = []
        if self.enable_vector_rag:
            try:
                q_docs = (
                    "MATCH (d:Document) "
                    "WHERE any(tag IN coalesce(d.tags, []) WHERE tag IN $tags) "
                    "RETURN d.doc_id AS id, d.title AS title, d.url AS url, d.summary AS summary "
                    "LIMIT $k"
                )
                tags = list({*companies, *sectors})
                rag_docs = self.neo.execute_query(q_docs, {"tags": tags, "k": self.top_k_docs})
            except Exception:
                logger.exception("Vector/BM25 document retrieval failed")

        context_blocks = []
        if news_rows:
            context_blocks.append("# Notícias Relacionadas (Neo4j)\n" + "\n".join([
                f"- {r.get('title','')} ({r.get('url','')})" for r in news_rows[: self.top_k_news]
            ]))
        if rag_docs:
            context_blocks.append("# Docs Relacionados (Neo4j)\n" + "\n".join([
                f"- {d.get('title','')} ({d.get('url','')}) — {d.get('summary','')[:180]}" for d in rag_docs[: self.top_k_docs]
            ]))
        state["graph_context"] = "\n\n".join(context_blocks)
        state["rag_docs"] = rag_docs
        return state

    def _step_analyze_profiles(self, state: dict) -> dict:
        companies = state.get("identified_companies", [])
        sectors = state.get("identified_sectors", [])
        identified_companies_str = ", ".join(companies) if companies else "Nenhuma específica"
        identified_sectors_str = ", ".join(sectors) if sectors else "Nenhum específico"

        if not companies and not sectors:
            entity_context = ("A notícia parece ter foco econômico geral, sem destacar empresas "
                              "ou setores específicos das listas conhecidas.")
        elif not companies:
            entity_context = f"Foco principal nos setores: [{identified_sectors_str}]."
        elif not sectors:
            entity_context = f"Foco principal nas empresas: [{identified_companies_str}]."
        else:
            entity_context = f"Foco principal em empresas: [{identified_companies_str}] e setores: [{identified_sectors_str}]."

        analysis_by_profile: Dict[str, Any] = {}
        for profile in self.profiles:
            human = self.prompts["human_impact_analysis"].format(
                perfil=profile,
                news=state["news_content"],
                identified_companies_str=identified_companies_str,
                identified_sectors_str=identified_sectors_str,
                entity_context=entity_context,
            )
            rag_human = (
                self.prompts.get("human_rag_wrapper", """\n===== CONTEXTO RAG (Neo4j) =====\n{graph_context}\n===== FIM CONTEXTO RAG =====\n\n{base_prompt}\n""")
            ).format(graph_context=state.get("graph_context", ""), base_prompt=human)
            system = self.prompts["system_impact_analysis"]
            try:
                resp = self.llm.invoke(system, rag_human)
                data = json.loads(resp)
                if not isinstance(data, dict) or "personal_score" not in data or "sector_score" not in data:
                    raise ValueError("Invalid analysis schema")
                analysis_by_profile[profile] = data
            except Exception:
                logger.exception("Profile analysis failed for %s", profile)
                analysis_by_profile[profile] = {"error": f"Falha ao gerar análise para perfil {profile}"}

        state["analysis_by_profile"] = analysis_by_profile
        return state

    def _step_persist(self, state: dict) -> dict:
        title = state["news_item"].get("title", f"unknown_{state['news_hash']}")
        filename = f"{clean_filename(title)[:60]}_multi_analysis.json"
        out_path = os.path.join(self.output_dir, filename)
        final_payload = {
            "news_title": state["news_item"].get("title", "N/A"),
            "news_url": state["news_item"].get("url", "N/A"),
            "news_date": state["news_item"].get("published_at", "N/A"),
            "news_hash": state["news_hash"],
            "news_snippet": state["news_content"][:250] + "...",
            "timestamp": dt.datetime.now().isoformat(),
            "identified_entities": {
                "companies": state.get("identified_companies", []),
                "sectors": state.get("identified_sectors", []),
            },
            "analysis_by_profile": state.get("analysis_by_profile", {}),
            "full_news_content": state.get("news_content", ""),
            "rag_context": state.get("graph_context", ""),
            "rag_docs": state.get("rag_docs", []),
        }
        try:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(final_payload, f, ensure_ascii=False, indent=2)
            state["output_path"] = out_path
        except Exception:
            logger.exception("Failed to write output JSON")

        if self.neo:
            try:
                self._save_to_neo4j(final_payload)
            except Exception:
                logger.exception("Neo4j persistence failed")
        return state

    def _save_to_neo4j(self, payload: Dict[str, Any]):
        news_hash = payload["news_hash"]
        merge_news = (
            "MERGE (n:News {news_hash: $news_hash})\n"
            "ON CREATE SET n.title=$title, n.url=$url, n.snippet=$snippet, n.full_content=$full_content, n.first_analyzed_at=datetime()\n"
            "ON MATCH SET n.last_analyzed_at=datetime(), n.url=$url, n.title=$title, n.snippet=$snippet\n"
            "RETURN elementId(n) AS id"
        )
        params = {
            "news_hash": news_hash,
            "title": payload["news_title"],
            "url": payload["news_url"],
            "snippet": payload["news_snippet"],
            "published_date": payload["news_snippet"],
            "full_content": payload["full_news_content"],
        }
        row = self.neo.execute_query(merge_news, params)[0]
        news_id = row["id"]

        companies = payload["identified_entities"].get("companies", [])
        sectors = payload["identified_entities"].get("sectors", [])

        for profile, analysis in payload["analysis_by_profile"].items():
            if "error" in analysis:
                continue
            props = {
                "profile": profile,
                "personal_score": analysis.get("personal_score"),
                "sector_score": analysis.get("sector_score"),
                "justification_personal": analysis.get("justification", {}).get("personal", ""),
                "justification_sector": analysis.get("justification", {}).get("sector", ""),
                "identified_companies_str": ", ".join(companies),
                "identified_sectors_str": ", ".join(sectors),
                "created_at": dt.datetime.now().isoformat(),
            }
            create_a = "CREATE (a:Analysis $props) RETURN elementId(a) AS id"
            aid = self.neo.execute_query(create_a, {"props": props})[0]["id"]

            link_a = (
                "MATCH (n:News), (a:Analysis) WHERE elementId(n)=$nid AND elementId(a)=$aid\n"
                "MERGE (n)-[:HAS_ANALYSIS]->(a)"
            )
            self.neo.execute_query(link_a, {"nid": news_id, "aid": aid})

            for cname in companies:
                q = (
                    "MATCH (c:Company {company_name: $name}), (a:Analysis) WHERE elementId(a)=$aid\n"
                    "MERGE (a)-[:ANALYZES_ENTITY {entity_type:'Company'}]->(c)"
                )
                self.neo.execute_query(q, {"name": cname, "aid": aid})

            for sname in sectors:
                q = (
                    "MERGE (ea:EconomicActivity {name:$name})\n"
                    "ON CREATE SET ea.type='Sector Focus', ea.created_at=datetime()\n"
                    "WITH ea\n"
                    "MATCH (a:Analysis) WHERE elementId(a)=$aid\n"
                    "MERGE (a)-[:ANALYZES_ACTIVITY {activity_type:'Sector Focus'}]->(ea)"
                )
                self.neo.execute_query(q, {"name": sname, "aid": aid})

        for doc in payload.get("rag_docs", [])[:10]:
            try:
                qd = (
                    "MERGE (d:Document {doc_id:$id})\n"
                    "ON CREATE SET d.title=$title, d.url=$url, d.summary=$summary\n"
                    "WITH d\n"
                    "MATCH (n:News) WHERE elementId(n)=$nid\n"
                    "MERGE (n)-[:USES_CONTEXT]->(d)"
                )
                self.neo.execute_query(qd, {
                    "id": doc.get("id"),
                    "title": doc.get("title"),
                    "url": doc.get("url"),
                    "published_date": doc.get("published_date"),
                    "summary": doc.get("summary", ""),
                    "nid": news_id,
                })
            except Exception:
                logger.warning("Failed to persist RAG doc link: %s", doc.get("id"))

class NewsProcessor:
    def __init__(self, llm_client: AIClient, neo4j_conn: Optional[Neo4jConnection], prompt_paths: Dict[str, str], output_dir: str = "output/analysis"):
        self.graph = NewsRAGGraph(llm_client, neo4j_conn, prompt_paths, output_dir)

    def process_news_item(self, news_item_data: Dict[str, str], processed_news_hashes: Set[int]) -> Optional[Dict[str, Any]]:
        content = f"{news_item_data.get('title','')}\n\n{news_item_data.get('article','')}"
        if not news_item_data.get("article"):
            logger.warning("Item de notícia sem conteúdo de artigo. Pulando.")
            return None
        h = hash(content)
        if h in processed_news_hashes:
            logger.info("Notícia duplicada (hash: %s) já processada. Pulando.", h)
            return None
        out = self.graph.run(news_item_data)
        processed_news_hashes.add(h)
        return out

    def process_news_batch(self, news_batch: List[Dict[str, str]]) -> List[Optional[Dict[str, Any]]]:
        results = []
        processed_news_hashes: Set[int] = set()
        for item in news_batch:
            try:
                res = self.process_news_item(item, processed_news_hashes)
                if res:
                    results.append(res)
            except Exception:
                logger.exception("Falha ao processar item do lote")
        return results
