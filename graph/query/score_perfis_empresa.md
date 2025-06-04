//Score + Perfil + Empresa
MATCH (c:Company) // Seleciona todas as empresas

// Opcional: Encontra análises onde esta empresa é a entidade analisada
OPTIONAL MATCH (c)<-[r_ent:ANALYZES_ENTITY {entity_type: 'Company'}]-(a:Analysis)
// 'a' será null para empresas sem nenhuma análise correspondente

// Agrupa por empresa e perfil, ordenando as análises dentro de cada grupo
// para que a mais recente possa ser facilmente selecionada.
WITH c, a
ORDER BY
    c.company_name ASC,
    // Só ordena por perfil se 'a' (análise) existir
    CASE WHEN a IS NOT NULL THEN a.profile ELSE NULL END ASC,
    // Para a data, primeiro ordena por se a data é nula (nulls primeiro se ASC, então fazemos DESC para nulls virem "depois" dos não-nulos na ordem DESC)
    // Ou, mais simples: ordena por um booleano que indica se 'a' ou 'a.analyzed_at' é null, depois pela data.
    // Para DESC NULLS LAST: ordena por (a.analyzed_at IS NULL) ASC, depois por a.analyzed_at DESC
    (a.analyzed_at IS NULL) ASC, // Garante que linhas com 'a.analyzed_at' nulo (ou 'a' nulo) fiquem por último na ordem DESC
    CASE WHEN a IS NOT NULL AND a.analyzed_at IS NOT NULL THEN datetime(a.analyzed_at) ELSE NULL END DESC

// Para cada empresa e cada perfil de análise encontrado, coleta todas as análises.
// Devido ao ORDER BY anterior, a primeira análise na lista será a mais recente para esse perfil.
WITH c,
     // Só define um nome de perfil se 'a' existir
     CASE WHEN a IS NOT NULL THEN a.profile ELSE NULL END AS profile_name,
     // Coleta os nós de análise. Se 'a' for null, COLLECT(a) resultará em [null]
     COLLECT(a) AS analyses_for_this_profile

// Filtra as linhas onde não havia perfil (ou seja, a empresa não tinha análises ou o perfil era nulo)
// E garante que a lista de análises não está vazia e o primeiro item não é nulo
WHERE profile_name IS NOT NULL AND SIZE(analyses_for_this_profile) > 0 AND analyses_for_this_profile[0] IS NOT NULL

// Pega a análise mais recente (a primeira da lista coletada, que foi ordenada)
WITH c, profile_name, analyses_for_this_profile[0] AS latest_analysis_for_profile

// Agora, para cada empresa, coleta os detalhes da análise mais recente de cada perfil
WITH c, COLLECT({
    profile: profile_name,
    latest_score: COALESCE(latest_analysis_for_profile.sector_score, latest_analysis_for_profile.personal_score, 0), // Usa sector_score ou personal_score
    analyzed_at: latest_analysis_for_profile.analyzed_at,
    analysis_id: elementId(latest_analysis_for_profile) // Opcional: ID da análise
}) AS scores_por_perfil_list

RETURN c.company_name AS empresa,
       scores_por_perfil_list AS scores_por_perfil
ORDER BY empresa ASC