from neo4j import GraphDatabase
import logging
from tabulate import tabulate

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class Neo4jConnection:
    def __init__(self, uri, user, pwd):
        try:
            self.driver = GraphDatabase.driver(uri, auth=(user, pwd))
            self.driver.verify_connectivity()
            logger.info("Successfully connected to Neo4j database.")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j at {uri}: {e}")
            raise

    def close(self):
        if self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed.")

    def _execute_query(self, query, parameters=None):
        if not self.driver:
            logger.error("Database driver not initialized.")
            return []
        try:
            with self.driver.session() as session:
                results = session.run(query, parameters)
                return [dict(record) for record in results]
        except Exception as e:
            logger.error(f"Error executing query: {e}", exc_info=True)
            return []

    def create_node(self, label, properties):
        with self.driver.session() as session:
            query = f"CREATE (n:{label} $props) RETURN elementId(n) AS id, n AS node"
            result = session.run(query, props=properties)
            record = result.single()
            return record["id"] if record else None

    def create_relationship(self, node1_element_id, node2_element_id, rel_type, properties=None):
        with self.driver.session() as session:
            query = (
                f"MATCH (a), (b) "
                f"WHERE elementId(a) = $node1_element_id AND elementId(b) = $node2_element_id "
                f"CREATE (a)-[r:{rel_type} $props]->(b) "
                f"RETURN r"
            )
            result = session.run(query, node1_element_id=node1_element_id, node2_element_id=node2_element_id, props=properties or {})
            single_result = result.single()
            return single_result[0] if single_result else None

    def get_company_focused_analysis(self, nome_empresa_principal: str):
        query1 = """
        WITH $nome_empresa_principal_param AS nome_empresa_principal
        MATCH (principal_company:Company {company_name: nome_empresa_principal})
        MATCH (principal_company)<-[r_ent:ANALYZES_ENTITY]-(analysis:Analysis)
        WHERE r_ent.entity_type = 'Company'
        WITH principal_company, analysis, datetime(analysis.analyzed_at).epochMillis AS analyzed_at_epoch
        WITH principal_company, COLLECT({analysis_node: analysis, analyzed_at_epoch: analyzed_at_epoch, score: COALESCE(analysis.sector_score, analysis.personal_score)}) AS direct_analyses_list
        WITH principal_company, direct_analyses_list,
             CASE WHEN size(direct_analyses_list) > 0 THEN REDUCE(m = direct_analyses_list[0].analyzed_at_epoch, item IN direct_analyses_list | CASE WHEN item.analyzed_at_epoch < m THEN item.analyzed_at_epoch ELSE m END) ELSE NULL END AS min_direct_epoch,
             CASE WHEN size(direct_analyses_list) > 0 THEN REDUCE(m = direct_analyses_list[0].analyzed_at_epoch, item IN direct_analyses_list | CASE WHEN item.analyzed_at_epoch > m THEN item.analyzed_at_epoch ELSE m END) ELSE NULL END AS max_direct_epoch
        UNWIND direct_analyses_list AS direct_data
        WITH principal_company,
             direct_data.analysis_node AS direct_analysis,
             direct_data.score AS direct_score,
             direct_data.analyzed_at_epoch AS direct_analyzed_at_epoch,
             min_direct_epoch,
             max_direct_epoch,
             CASE
                  WHEN max_direct_epoch IS NOT NULL AND min_direct_epoch IS NOT NULL AND max_direct_epoch - min_direct_epoch > 0
                  THEN (toFloat(direct_data.analyzed_at_epoch - min_direct_epoch) / (max_direct_epoch - min_direct_epoch))
                  ELSE 1.0
             END + 0.1 AS direct_recency_weight
        WITH principal_company,
             direct_analysis.profile AS perfil,
             SUM(direct_score * direct_recency_weight) AS weighted_score_sum_principal_company,
             SUM(direct_recency_weight) AS total_weight_principal_company,
             COUNT(direct_analysis) AS num_analyses_principal_company
        WITH principal_company,
             perfil,
             num_analyses_principal_company,
             CASE
                  WHEN total_weight_principal_company > 0
                  THEN weighted_score_sum_principal_company / total_weight_principal_company
                  ELSE 0
             END AS avg_weighted_score_principal_company
        OPTIONAL MATCH (principal_company)-[cor:HAS_CORRELATION_WITH]-(correlatedCompany:Company)
        WITH principal_company, perfil, num_analyses_principal_company, avg_weighted_score_principal_company,
             correlatedCompany, cor,
             principal_company.sector AS principal_company_sector_name
        OPTIONAL MATCH (sector_activity:EconomicActivity {name: principal_company_sector_name})
        OPTIONAL MATCH (sector_activity)<-[r_act_sec:ANALYZES_ACTIVITY]-(analysis_sector:Analysis)
        WHERE r_act_sec.activity_type = 'Sector Focus' AND analysis_sector.profile = perfil
        WITH principal_company, perfil, num_analyses_principal_company, avg_weighted_score_principal_company,
             correlatedCompany, cor, principal_company_sector_name,
             CASE WHEN analysis_sector IS NOT NULL THEN datetime(analysis_sector.analyzed_at).epochMillis ELSE NULL END AS sector_analyzed_at_epoch,
             analysis_sector
        WITH principal_company, perfil, num_analyses_principal_company, avg_weighted_score_principal_company,
             correlatedCompany, cor, principal_company_sector_name,
             COLLECT(DISTINCT CASE WHEN analysis_sector IS NOT NULL THEN {
                 score: COALESCE(analysis_sector.sector_score, analysis_sector.personal_score),
                 analyzed_at_epoch: sector_analyzed_at_epoch
             } ELSE NULL END) AS sector_analyses_data_list_raw
        WITH principal_company, perfil, num_analyses_principal_company, avg_weighted_score_principal_company,
             correlatedCompany, cor, principal_company_sector_name,
             [data IN sector_analyses_data_list_raw WHERE data IS NOT NULL] AS sector_analyses_data,
             CASE WHEN SIZE([data IN sector_analyses_data_list_raw WHERE data IS NOT NULL]) > 0 THEN REDUCE(m = [data IN sector_analyses_data_list_raw WHERE data IS NOT NULL][0].analyzed_at_epoch, item IN [data IN sector_analyses_data_list_raw WHERE data IS NOT NULL] | CASE WHEN item.analyzed_at_epoch < m THEN item.analyzed_at_epoch ELSE m END) ELSE NULL END AS min_sector_epoch,
             CASE WHEN SIZE([data IN sector_analyses_data_list_raw WHERE data IS NOT NULL]) > 0 THEN REDUCE(m = [data IN sector_analyses_data_list_raw WHERE data IS NOT NULL][0].analyzed_at_epoch, item IN [data IN sector_analyses_data_list_raw WHERE data IS NOT NULL] | CASE WHEN item.analyzed_at_epoch > m THEN item.analyzed_at_epoch ELSE m END) ELSE NULL END AS max_sector_epoch
        UNWIND (CASE WHEN SIZE(sector_analyses_data) = 0 THEN [null] ELSE sector_analyses_data END) AS sector_data_item
        WITH principal_company, perfil, num_analyses_principal_company, avg_weighted_score_principal_company,
             correlatedCompany, cor, principal_company_sector_name,
             sector_data_item, min_sector_epoch, max_sector_epoch,
             CASE
                WHEN sector_data_item IS NULL THEN NULL
                WHEN max_sector_epoch IS NOT NULL AND min_sector_epoch IS NOT NULL AND max_sector_epoch - min_sector_epoch > 0
                THEN (toFloat(sector_data_item.analyzed_at_epoch - min_sector_epoch) / (max_sector_epoch - min_sector_epoch))
                WHEN sector_data_item IS NOT NULL THEN 1.0
                ELSE NULL
             END + CASE WHEN sector_data_item IS NOT NULL THEN 0.1 ELSE 0.0 END AS sector_recency_weight
        WITH principal_company, perfil, num_analyses_principal_company, avg_weighted_score_principal_company,
             principal_company_sector_name,
             SUM(CASE WHEN sector_data_item IS NOT NULL AND sector_recency_weight IS NOT NULL THEN sector_data_item.score * sector_recency_weight ELSE 0 END) AS weighted_score_sum_sector,
             SUM(CASE WHEN sector_data_item IS NOT NULL AND sector_recency_weight IS NOT NULL THEN sector_recency_weight ELSE 0 END) AS total_weight_sector,
             COUNT(CASE WHEN sector_data_item IS NOT NULL THEN true ELSE NULL END) AS num_analyses_sector,
             COLLECT(DISTINCT CASE WHEN correlatedCompany IS NOT NULL THEN {
                 name: correlatedCompany.company_name,
                 correlation_level: cor.correlation_level,
                 impact_score: avg_weighted_score_principal_company * cor.correlation_level
             } ELSE NULL END) AS correlated_companies_details_list
        WITH principal_company, perfil, num_analyses_principal_company, avg_weighted_score_principal_company,
             principal_company_sector_name,
             weighted_score_sum_sector, total_weight_sector, num_analyses_sector,
             [item IN correlated_companies_details_list WHERE item IS NOT NULL] AS correlated_companies_details
        RETURN
            perfil,
            principal_company.company_name AS empresa_principal,
            avg_weighted_score_principal_company AS score_ponderado_empresa_principal,
            num_analyses_principal_company,
            principal_company_sector_name AS setor_da_empresa_principal,
            CASE
                WHEN total_weight_sector > 0 THEN weighted_score_sum_sector / total_weight_sector
                ELSE NULL
            END AS score_ponderado_direto_do_setor,
            num_analyses_sector AS numero_analises_diretas_setor,
            correlated_companies_details AS empresas_correlacionadas_impactadas
        ORDER BY perfil, avg_weighted_score_principal_company DESC
        """
        parameters = {"nome_empresa_principal_param": nome_empresa_principal}
        return self._execute_query(query1, parameters)

    def get_sector_focused_analysis(self, nome_atividade_principal: str, peso_atividade_direta: float = 0.5):
        query2 = """
        WITH $peso_atividade_direta_param AS w_activity, $nome_atividade_param AS nome_atividade
        OPTIONAL MATCH (activity_node:EconomicActivity {name: nome_atividade})<-[r_act:ANALYZES_ACTIVITY]-(analysis_act:Analysis)
        WHERE r_act.activity_type = 'Sector Focus'
        WITH activity_node, analysis_act, nome_atividade, w_activity,
             CASE WHEN analysis_act IS NOT NULL THEN datetime(analysis_act.analyzed_at).epochMillis ELSE NULL END AS analyzed_at_epoch_act
        WITH activity_node, nome_atividade, w_activity,
             COLLECT(DISTINCT CASE WHEN analysis_act IS NOT NULL THEN {
                 analysis_node: analysis_act,
                 analyzed_at_epoch: analyzed_at_epoch_act,
                 score: COALESCE(analysis_act.sector_score, analysis_act.personal_score)
             } ELSE NULL END) AS direct_activity_analyses_list_raw
        WITH activity_node, nome_atividade, w_activity,
             [item IN direct_activity_analyses_list_raw WHERE item IS NOT NULL] AS direct_activity_analyses_data,
             CASE WHEN SIZE([item IN direct_activity_analyses_list_raw WHERE item IS NOT NULL]) > 0 THEN REDUCE(m = [item IN direct_activity_analyses_list_raw WHERE item IS NOT NULL][0].analyzed_at_epoch, item IN [item IN direct_activity_analyses_list_raw WHERE item IS NOT NULL] | CASE WHEN item.analyzed_at_epoch < m THEN item.analyzed_at_epoch ELSE m END) ELSE NULL END AS min_act_epoch,
             CASE WHEN SIZE([item IN direct_activity_analyses_list_raw WHERE item IS NOT NULL]) > 0 THEN REDUCE(m = [item IN direct_activity_analyses_list_raw WHERE item IS NOT NULL][0].analyzed_at_epoch, item IN [item IN direct_activity_analyses_list_raw WHERE item IS NOT NULL] | CASE WHEN item.analyzed_at_epoch > m THEN item.analyzed_at_epoch ELSE m END) ELSE NULL END AS max_act_epoch
        UNWIND (CASE WHEN SIZE(direct_activity_analyses_data) = 0 THEN [null] ELSE direct_activity_analyses_data END) AS direct_act_data
        WITH activity_node, nome_atividade, w_activity, direct_act_data,
             min_act_epoch, max_act_epoch,
             CASE
                WHEN direct_act_data IS NULL THEN NULL
                WHEN max_act_epoch IS NOT NULL AND min_act_epoch IS NOT NULL AND max_act_epoch - min_act_epoch > 0
                THEN (toFloat(direct_act_data.analyzed_at_epoch - min_act_epoch) / (max_act_epoch - min_act_epoch))
                WHEN direct_act_data IS NOT NULL THEN 1.0
                ELSE NULL
             END + CASE WHEN direct_act_data IS NOT NULL THEN 0.1 ELSE 0.0 END AS direct_act_recency_weight
        WITH activity_node, nome_atividade, w_activity,
             CASE WHEN direct_act_data IS NOT NULL THEN direct_act_data.analysis_node.profile ELSE NULL END AS perfil_act,
             SUM(CASE WHEN direct_act_data IS NOT NULL AND direct_act_recency_weight IS NOT NULL THEN direct_act_data.score * direct_act_recency_weight ELSE 0 END) AS weighted_score_sum_act,
             SUM(CASE WHEN direct_act_data IS NOT NULL AND direct_act_recency_weight IS NOT NULL THEN direct_act_recency_weight ELSE 0 END) AS total_weight_act,
             COUNT(CASE WHEN direct_act_data IS NOT NULL THEN direct_act_data.analysis_node ELSE NULL END) AS num_analyses_act_val
        WITH activity_node, nome_atividade, w_activity,
             COLLECT(DISTINCT {
                 perfil: perfil_act,
                 score: CASE WHEN total_weight_act > 0 THEN weighted_score_sum_act / total_weight_act ELSE NULL END,
                 num_analyses: num_analyses_act_val
             }) AS direct_activity_scores_by_profile_list
        OPTIONAL MATCH (company_in_sector:Company {sector: nome_atividade})<-[r_ent_comp:ANALYZES_ENTITY]-(analysis_comp:Analysis)
        WHERE r_ent_comp.entity_type = 'Company'
        WITH company_in_sector, analysis_comp, nome_atividade, w_activity, direct_activity_scores_by_profile_list, activity_node,
             CASE WHEN analysis_comp IS NOT NULL THEN datetime(analysis_comp.analyzed_at).epochMillis ELSE NULL END AS analyzed_at_epoch_comp
        WITH nome_atividade, w_activity, direct_activity_scores_by_profile_list, activity_node,
             COLLECT(DISTINCT CASE WHEN analysis_comp IS NOT NULL THEN {
                 analysis_node: analysis_comp,
                 company_node: company_in_sector,
                 analyzed_at_epoch: analyzed_at_epoch_comp,
                 score: COALESCE(analysis_comp.sector_score, analysis_comp.personal_score)
             } ELSE NULL END) AS company_analyses_list_raw
        WITH nome_atividade, w_activity, direct_activity_scores_by_profile_list, activity_node,
             [item IN company_analyses_list_raw WHERE item IS NOT NULL] AS company_analyses_data,
             CASE WHEN SIZE([item IN company_analyses_list_raw WHERE item IS NOT NULL]) > 0 THEN REDUCE(m = [item IN company_analyses_list_raw WHERE item IS NOT NULL][0].analyzed_at_epoch, item IN [item IN company_analyses_list_raw WHERE item IS NOT NULL] | CASE WHEN item.analyzed_at_epoch < m THEN item.analyzed_at_epoch ELSE m END) ELSE NULL END AS min_comp_epoch,
             CASE WHEN SIZE([item IN company_analyses_list_raw WHERE item IS NOT NULL]) > 0 THEN REDUCE(m = [item IN company_analyses_list_raw WHERE item IS NOT NULL][0].analyzed_at_epoch, item IN [item IN company_analyses_list_raw WHERE item IS NOT NULL] | CASE WHEN item.analyzed_at_epoch > m THEN item.analyzed_at_epoch ELSE m END) ELSE NULL END AS max_comp_epoch
        UNWIND (CASE WHEN SIZE(company_analyses_data) = 0 THEN [null] ELSE company_analyses_data END) AS comp_data
        WITH nome_atividade, w_activity, direct_activity_scores_by_profile_list, activity_node, comp_data,
             min_comp_epoch, max_comp_epoch,
             CASE
                WHEN comp_data IS NULL THEN NULL
                WHEN max_comp_epoch IS NOT NULL AND min_comp_epoch IS NOT NULL AND max_comp_epoch - min_comp_epoch > 0
                THEN (toFloat(comp_data.analyzed_at_epoch - min_comp_epoch) / (max_comp_epoch - min_comp_epoch))
                WHEN comp_data IS NOT NULL THEN 1.0
                ELSE NULL
             END + CASE WHEN comp_data IS NOT NULL THEN 0.1 ELSE 0.0 END AS comp_recency_weight
        WITH nome_atividade, w_activity, direct_activity_scores_by_profile_list, activity_node,
             CASE WHEN comp_data IS NOT NULL THEN comp_data.analysis_node.profile ELSE NULL END AS perfil_comp,
             SUM(CASE WHEN comp_data IS NOT NULL AND comp_recency_weight IS NOT NULL THEN comp_data.score * comp_recency_weight ELSE 0 END) AS weighted_score_sum_comp,
             SUM(CASE WHEN comp_data IS NOT NULL AND comp_recency_weight IS NOT NULL THEN comp_recency_weight ELSE 0 END) AS total_weight_comp,
             COUNT(CASE WHEN comp_data IS NOT NULL THEN comp_data.analysis_node ELSE NULL END) AS num_analyses_comp_val
        WITH nome_atividade, w_activity, direct_activity_scores_by_profile_list, activity_node,
             COLLECT(DISTINCT {
                 perfil: perfil_comp,
                 score: CASE WHEN total_weight_comp > 0 THEN weighted_score_sum_comp / total_weight_comp ELSE NULL END,
                 num_analyses: num_analyses_comp_val
             }) AS company_scores_by_profile_list
        WITH ['Conservador', 'Moderado', 'Agressivo'] AS all_profiles,
             nome_atividade, w_activity, direct_activity_scores_by_profile_list, company_scores_by_profile_list, activity_node
        UNWIND all_profiles AS perfil_final
        WITH perfil_final, nome_atividade, w_activity, activity_node,
             HEAD([s IN direct_activity_scores_by_profile_list WHERE s.perfil = perfil_final AND s.score IS NOT NULL]) AS direct_activity_score_obj,
             HEAD([s IN company_scores_by_profile_list WHERE s.perfil = perfil_final AND s.score IS NOT NULL]) AS company_agg_score_obj
        WITH perfil_final, nome_atividade, w_activity, activity_node,
             COALESCE(direct_activity_score_obj.score, NULL) AS score_direto_atividade,
             COALESCE(direct_activity_score_obj.num_analyses, 0) AS num_analises_diretas_atividade,
             COALESCE(company_agg_score_obj.score, NULL) AS score_agregado_empresas,
             COALESCE(company_agg_score_obj.num_analyses, 0) AS num_analises_empresas_no_setor
        WITH perfil_final, nome_atividade, w_activity, activity_node,
             score_direto_atividade,
             num_analises_diretas_atividade,
             score_agregado_empresas,
             num_analises_empresas_no_setor,
             CASE
                 WHEN score_direto_atividade IS NOT NULL AND score_agregado_empresas IS NOT NULL
                     THEN (score_direto_atividade * w_activity) + (score_agregado_empresas * (1.0 - w_activity))
                 WHEN score_direto_atividade IS NOT NULL
                     THEN score_direto_atividade
                 WHEN score_agregado_empresas IS NOT NULL
                     THEN score_agregado_empresas
                 ELSE NULL
             END AS combined_sector_score
        WHERE combined_sector_score IS NOT NULL AND activity_node IS NOT NULL
        OPTIONAL MATCH (activity_node)<-[dep:DEPENDS_ON]-(dependentCompany:Company)
        WITH perfil_final, nome_atividade, combined_sector_score,
             num_analises_diretas_atividade, num_analises_empresas_no_setor,
             COLLECT(DISTINCT CASE WHEN dependentCompany IS NOT NULL AND combined_sector_score IS NOT NULL THEN {
                 name: dependentCompany.company_name,
                 dependency_level: dep.dependency_level,
                 impact_score: combined_sector_score * dep.dependency_level
             } ELSE NULL END) AS dependent_companies_details_list_raw
        WITH perfil_final AS perfil, nome_atividade AS atividade_principal_analisada,
             combined_sector_score AS score_final_combinado_setor,
             num_analises_diretas_atividade, num_analises_empresas_no_setor,
             [item IN dependent_companies_details_list_raw WHERE item IS NOT NULL] AS empresas_dependentes_impactadas
        RETURN
            perfil,
            atividade_principal_analisada,
            score_final_combinado_setor,
            num_analises_diretas_atividade,
            num_analises_empresas_no_setor,
            empresas_dependentes_impactadas
        ORDER BY perfil, score_final_combinado_setor DESC
        """
        parameters = {
            "nome_atividade_param": nome_atividade_principal,
            "peso_atividade_direta_param": peso_atividade_direta
        }
        return self._execute_query(query2, parameters)

'''if __name__ == '__main__':
    NEO4J_URI = ""
    NEO4J_USER = ""
    NEO4J_PASSWORD = ""

    conn = None
    try:
        conn = Neo4jConnection(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

        logger.info("\n=== Company Analysis: Amazon.com Inc BDR ===")
        company_analysis_results = conn.get_company_focused_analysis(nome_empresa_principal="Amazon.com Inc BDR")
        
        if company_analysis_results:
            headers = [
                "Profile",
                "Company",
                "Weighted Score",
                "Number of Analyses",
                "Sector",
                "Sector Weighted Score",
                "Sector Analyses",
                "Correlated Companies"
            ]
            table_data = []
            for record in company_analysis_results:
                correlated_companies = record.get("empresas_correlacionadas_impactadas", [])
                correlated_str = "; ".join(
                    [f"{c['name']} (Correlation: {c['correlation_level']:.2f}, Impact: {c['impact_score']:.2f})"
                     for c in correlated_companies] if correlated_companies else ["None"]
                )
                table_data.append([
                    record.get("perfil", "N/A"),
                    record.get("empresa_principal", "N/A"),
                    f"{record.get('score_ponderado_empresa_principal', 0):.2f}",
                    record.get("num_analyses_principal_company", 0),
                    record.get("setor_da_empresa_principal", "N/A"),
                    f"{record.get('score_ponderado_direto_do_setor', 0):.2f}" if record.get("score_ponderado_direto_do_setor") is not None else "N/A",
                    record.get("numero_analises_diretas_setor", 0),
                    correlated_str
                ])
            logger.info("\n" + tabulate(table_data, headers=headers, tablefmt="grid"))
        else:
            logger.warning(
                "No results found for company analysis of 'Amazon.com Inc BDR'. "
                "Verify that the company exists in the database with associated analyses."
            )

        logger.info("\n=== Sector Analysis: Comércio Varejista ===")
        sector_analysis_results = conn.get_sector_focused_analysis(nome_atividade_principal="Comércio Varejista", peso_atividade_direta=0.6)
        
        if sector_analysis_results:
            headers = [
                "Profile",
                "Sector",
                "Combined Score",
                "Direct Analyses",
                "Company Analyses",
                "Dependent Companies"
            ]
            table_data = []
            for record in sector_analysis_results:
                dependent_companies = record.get("empresas_dependentes_impactadas", [])
                dependent_str = "; ".join(
                    [f"{c['name']} (Dependency: {c['dependency_level']:.2f}, Impact: {c['impact_score']:.2f})"
                     for c in dependent_companies] if dependent_companies else ["None"]
                )
                table_data.append([
                    record.get("perfil", "N/A"),
                    record.get("atividade_principal_analisada", "N/A"),
                    f"{record.get('score_final_combinado_setor', 0):.2f}",
                    record.get("num_analises_diretas_atividade", 0),
                    record.get("num_analises_empresas_no_setor", 0),
                    dependent_str
                ])
            logger.info("\n" + tabulate(table_data, headers=headers, tablefmt="grid"))
        else:
            logger.warning(
                "No results found for sector analysis of 'Comércio Varejista'. "
                "Verify that an EconomicActivity node with name 'Comércio Varejista' exists "
                "and has associated analyses (ANALYZES_ACTIVITY with activity_type='Sector Focus') "
                "or companies in the sector with analyses (ANALYZES_ENTITY with entity_type='Company')."
            )

    except Exception as e:
        logger.error(f"An error occurred during execution: {e}", exc_info=True)
    finally:
        if conn and conn.driver:
            conn.close()'''