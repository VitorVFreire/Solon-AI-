//Atividades com Analises
MATCH (e:EconomicActivity)<-[r]-(a:Analysis)
RETURN 
    e.name AS Atividade,
    type(r) AS relacao,
    count(a) AS qtd_analises
ORDER BY qtd_analises DESC;