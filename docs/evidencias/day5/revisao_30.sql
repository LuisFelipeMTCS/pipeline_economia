
/* 
3. Evolução mensal
  Revisão - 28/06/2024
  Questão: Como o faturamento evoluiu mês a mês ao longo do tempo?
*/

SELECT
  d.ano,
  d.mes,
  COUNT(f.id_nfe)              AS quantidade_nfe,
  ROUND(SUM(f.valor_total), 2) AS faturamento_total,
  ROUND(AVG(f.valor_total), 2) AS ticket_medio
FROM gold.fato_vendas f
JOIN gold.dim_data d ON f.id_data = d.id_data
GROUP BY d.ano, d.mes
ORDER BY d.ano, d.mes;
