//Score + Perfil + Setor
MATCH (ea:EconomicActivity)
// Opcional: Adicione um filtro se os seus "setores" tiverem uma propriedade específica,
// por exemplo: WHERE ea.type = 'Sector Focus'

// Opcional: Encontra análises onde esta atividade econômica é o foco
OPTIONAL MATCH (ea)<-[r_act:ANALYZES_ACTIVITY {activity_type: 'Sector Focus'}]-(a:Analysis)
// 'a' será null para atividades econômicas sem nenhuma análise correspondente

// Agrupa por atividade e perfil, ordenando as análises dentro de cada grupo
// para que a mais recente possa ser facilmente selecionada.
WITH ea, a
ORDER BY
    ea.name ASC,
    // Só ordena por perfil se 'a' (análise) existir
    CASE WHEN a IS NOT NULL THEN a.profile ELSE NULL END ASC,
    // Ordenação para tratar datas nulas (DESC NULLS LAST behavior)
    (a.analyzed_at IS NULL) ASC, // false (não nulo) vem antes de true (nulo)
    CASE WHEN a IS NOT NULL AND a.analyzed_at IS NOT NULL THEN datetime(a.analyzed_at) ELSE NULL END DESC

// Para cada atividade e cada perfil de análise encontrado, coleta todas as análises.
// Devido ao ORDER BY anterior, a primeira análise na lista será a mais recente para esse perfil.
WITH ea,
     // Só define um nome de perfil se 'a' existir
     CASE WHEN a IS NOT NULL THEN a.profile ELSE NULL END AS profile_name,
     // Coleta os nós de análise. Se 'a' for null, COLLECT(a) resultará em [null]
     COLLECT(a) AS analyses_for_this_profile

// Filtra as linhas onde não havia perfil (ou seja, a atividade não tinha análises ou o perfil era nulo)
// E garante que a lista de análises não está vazia e o primeiro item não é nulo
WHERE profile_name IS NOT NULL AND SIZE(analyses_for_this_profile) > 0 AND analyses_for_this_profile[0] IS NOT NULL

// Pega a análise mais recente (a primeira da lista coletada, que foi ordenada)
WITH ea, profile_name, analyses_for_this_profile[0] AS latest_analysis_for_profile

// Agora, para cada atividade, coleta os detalhes da análise mais recente de cada perfil
WITH ea, COLLECT({
    profile: profile_name,
    latest_score: COALESCE(latest_analysis_for_profile.sector_score, latest_analysis_for_profile.personal_score, 0), // Usa sector_score ou personal_score
    analyzed_at: latest_analysis_for_profile.analyzed_at,
    analysis_id: elementId(latest_analysis_for_profile) // Opcional: ID da análise
}) AS scores_por_perfil_list

RETURN ea.name AS atividade_economica, // ou setor
       scores_por_perfil_list AS scores_por_perfil
ORDER BY atividade_economica ASC