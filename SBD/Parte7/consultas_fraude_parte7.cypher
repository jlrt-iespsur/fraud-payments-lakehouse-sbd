// PARTE 7 - Consultas de investigación
// OJO: reemplazar <graph_name> por el snapshot que se quiera analizar.

// 1) Dispositivos compartidos por múltiples tarjetas.
MATCH (card:Card)-[:USED_ON]->(device:Device)
MATCH (card)-[:AUTHORIZED]->(payment:Payment)
WHERE payment.graph_name = '<graph_name>'
WITH device, collect(DISTINCT card.card_id) AS cards, count(DISTINCT card) AS total_cards
WHERE total_cards >= 3
RETURN device.device_id, total_cards, cards
ORDER BY total_cards DESC;

// 2) Tarjetas utilizadas en múltiples países.
MATCH (card:Card)-[:AUTHORIZED]->(payment:Payment)
WHERE payment.graph_name = '<graph_name>'
WITH card, collect(DISTINCT payment.country) AS countries, count(DISTINCT payment.country) AS total_countries
WHERE total_countries >= 3
RETURN card.card_id, total_countries, countries
ORDER BY total_countries DESC;

// 3) Comercios conectados a múltiples entidades sospechosas.
MATCH (customer:Customer)-[:OWNS_CARD]->(card:Card)-[:AUTHORIZED]->(payment:Payment {is_alert: true})-[:AT_MERCHANT]->(merchant:Merchant)
WHERE payment.graph_name = '<graph_name>'
WITH merchant, count(DISTINCT customer) AS suspicious_customers, count(DISTINCT card) AS suspicious_cards
WHERE suspicious_customers >= 2 OR suspicious_cards >= 3
RETURN merchant.merchant_id, suspicious_customers, suspicious_cards
ORDER BY suspicious_customers DESC, suspicious_cards DESC;

// 4) Posibles agrupaciones de comportamiento anómalo.
MATCH (card:Card)-[:USED_ON]->(device:Device)<-[:USED_ON]-(other_card:Card)
MATCH (other_card)-[:AUTHORIZED]->(payment:Payment {is_alert: true})
WHERE card.card_id <> other_card.card_id
  AND payment.graph_name = '<graph_name>'
RETURN device.device_id, collect(DISTINCT other_card.card_id) AS cards, count(DISTINCT payment) AS alert_payments
ORDER BY alert_payments DESC;
