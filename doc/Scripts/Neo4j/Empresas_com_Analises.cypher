//Empresas com Analises
MATCH (c:Company)<-[r]-(a:Analysis)
RETURN 
    c.company_name AS empresa,
    type(r) AS relacao,
    count(a) AS qtd_analises,
    collect(DISTINCT a.profile) AS perfis
ORDER BY qtd_analises DESC;

