/* 
1. Vendas por UF
  Revisão - 28/06/2024
  Questão: Quais estados geram mais faturamento e qual o ticket médio por NF-e emitida?
*/

SELECT
  l.uf,
  l.regiao,
  COUNT(f.id_nfe)              AS quantidade_nfe,
  ROUND(SUM(f.valor_total), 2) AS faturamento_total,
  ROUND(AVG(f.valor_total), 2) AS ticket_medio
FROM gold.fato_vendas f
JOIN gold.dim_localidade l ON f.id_localidade = l.id_localidade
GROUP BY l.uf, l.regiao
ORDER BY faturamento_total DESC;
