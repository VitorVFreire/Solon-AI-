//Empresa em Linha de Tempo

MATCH (principal_company:Company {company_name: $company})
OPTIONAL MATCH (principal_company)<-[r_ent:ANALYZES_ENTITY {entity_type: 'Company'}]-(pc_analysis:Analysis)
WITH principal_company, pc_analysis
ORDER BY principal_company.company_name, 
         CASE WHEN pc_analysis IS NOT NULL THEN pc_analysis.profile ELSE NULL END ASC, 
         CASE WHEN pc_analysis IS NOT NULL THEN datetime(pc_analysis.analyzed_at) ELSE NULL END ASC
WITH principal_company,
     CASE WHEN pc_analysis IS NOT NULL THEN pc_analysis.profile ELSE NULL END AS pc_profile,
     COLLECT(
        CASE 
            WHEN pc_analysis IS NOT NULL THEN {
                date: pc_analysis.analyzed_at,
                score: COALESCE(pc_analysis.sector_score, pc_analysis.personal_score, 0),
                analysis_id: elementId(pc_analysis)
            } 
            ELSE NULL 
        END
     ) AS pc_score_time_series_raw_for_profile
WITH principal_company, 
     COLLECT(DISTINCT CASE 
                        WHEN pc_profile IS NOT NULL THEN {
                            profile: pc_profile, 
                            time_series: [ts_item IN pc_score_time_series_raw_for_profile WHERE ts_item IS NOT NULL]
                        } 
                        ELSE NULL 
                      END
     ) AS principal_company_scores_by_profile_raw
WITH principal_company,
     [item IN principal_company_scores_by_profile_raw WHERE item IS NOT NULL AND SIZE(item.time_series) > 0] AS principal_company_profile_scores
OPTIONAL MATCH (principal_company)-[cor:HAS_CORRELATION_WITH]-(correlated_company:Company)
UNWIND (CASE 
            WHEN SIZE(principal_company_profile_scores) > 0 THEN principal_company_profile_scores 
            ELSE [{profile: null, time_series: null}] 
        END) AS pc_profile_data
WITH principal_company, 
     pc_profile_data.profile AS current_pc_profile,         // Alias para output
     pc_profile_data.time_series AS current_pc_time_series,  // Alias para output
     COLLECT(
        DISTINCT CASE 
            WHEN correlated_company IS NOT NULL AND pc_profile_data.time_series IS NOT NULL THEN { // <--- MUDANÇA AQUI
                correlated_company_name: correlated_company.company_name,
                correlation_level: cor.correlation_level,
                impacted_time_series: [ts_item IN pc_profile_data.time_series | { // <--- E AQUI
                    date_of_principal_analysis: ts_item.date,
                    impacted_score: ts_item.score * cor.correlation_level,
                    principal_analysis_id: ts_item.analysis_id 
                }]
            } 
            ELSE NULL 
        END
     ) AS correlated_impacts_raw_for_profile
WITH principal_company.company_name AS empresa_principal_nome,
     COLLECT(DISTINCT CASE 
                        WHEN current_pc_profile IS NOT NULL THEN {
                            profile: current_pc_profile,
                            score_time_series_principal: current_pc_time_series, // Usa o alias aqui, pois já foi definido e agrupado
                            correlated_company_impacts: [item IN correlated_impacts_raw_for_profile WHERE item IS NOT NULL]
                        } 
                        ELSE NULL 
                      END
     ) AS all_profile_data_raw
RETURN empresa_principal_nome,
       [item IN all_profile_data_raw WHERE item IS NOT NULL] AS dados_por_perfil
ORDER BY empresa_principal_nome