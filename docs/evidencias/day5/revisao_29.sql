/* 
2. Top emitentes
  Revisão - 28/06/2024
  Questão: Quais são os 10 maiores emitentes em volume de vendas e de qual estado são?
*/



SELECT
  e.cnpj,
  e.nome,
  l.uf,
  COUNT(f.id_nfe)              AS quantidade_nfe,
  ROUND(SUM(f.valor_total), 2) AS faturamento_total,
  ROUND(AVG(f.valor_total), 2) AS ticket_medio
FROM gold.fato_vendas f
JOIN gold.dim_emitente   e ON f.id_emitente   = e.id_emitente
JOIN gold.dim_localidade l ON f.id_localidade = l.id_localidade
GROUP BY e.cnpj, e.nome, l.uf
ORDER BY faturamento_total DESC
LIMIT 10;