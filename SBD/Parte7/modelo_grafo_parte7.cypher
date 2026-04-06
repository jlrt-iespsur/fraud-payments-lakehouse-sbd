// PARTE 7 - Modelo de grafo para fraude
// OJO: reemplazar <graph_name> por el snapshot que se quiera cargar.

// 1) Restricciones
CREATE CONSTRAINT customer_id IF NOT EXISTS FOR (c:Customer) REQUIRE c.customer_id IS UNIQUE;
CREATE CONSTRAINT card_id IF NOT EXISTS FOR (c:Card) REQUIRE c.card_id IS UNIQUE;
CREATE CONSTRAINT device_id IF NOT EXISTS FOR (d:Device) REQUIRE d.device_id IS UNIQUE;
CREATE CONSTRAINT merchant_id IF NOT EXISTS FOR (m:Merchant) REQUIRE m.merchant_id IS UNIQUE;
CREATE CONSTRAINT payment_id IF NOT EXISTS FOR (p:Payment) REQUIRE p.payment_id IS UNIQUE;

// 2) Limpieza parcial del snapshot (solo pagos de ese grafo)
MATCH (p:Payment {graph_name: '<graph_name>'})
DETACH DELETE p;

// 3) Nodos
LOAD CSV WITH HEADERS FROM 'file:///<graph_name>/customers.csv' AS row
MERGE (:Customer {customer_id: row.customer_id});

LOAD CSV WITH HEADERS FROM 'file:///<graph_name>/cards.csv' AS row
MERGE (:Card {card_id: row.card_id});

LOAD CSV WITH HEADERS FROM 'file:///<graph_name>/devices.csv' AS row
MERGE (:Device {device_id: row.device_id});

LOAD CSV WITH HEADERS FROM 'file:///<graph_name>/merchants.csv' AS row
MERGE (m:Merchant {merchant_id: row.merchant_id})
SET m.country = row.country, m.mcc = row.mcc;

LOAD CSV WITH HEADERS FROM 'file:///<graph_name>/payments.csv' AS row
MERGE (p:Payment {payment_id: row.payment_id})
SET
  p.payment_group_id = row.payment_group_id,
  p.event_time = row.event_time,
  p.amount = toFloat(row.amount),
  p.currency = row.currency,
  p.status = row.status,
  p.country = row.country,
  p.risk_score = toInteger(row.risk_score),
  p.reasons = row.reasons,
  p.is_alert = CASE row.is_alert WHEN 'True' THEN true ELSE false END,
  p.ip = row.ip,
  p.graph_name = row.graph_name;

// 4) Relaciones
LOAD CSV WITH HEADERS FROM 'file:///<graph_name>/rel_customer_card.csv' AS row
MATCH (customer:Customer {customer_id: row.customer_id})
MATCH (card:Card {card_id: row.card_id})
MERGE (customer)-[:OWNS_CARD]->(card);

LOAD CSV WITH HEADERS FROM 'file:///<graph_name>/rel_card_device.csv' AS row
MATCH (card:Card {card_id: row.card_id})
MATCH (device:Device {device_id: row.device_id})
MERGE (card)-[:USED_ON]->(device);

LOAD CSV WITH HEADERS FROM 'file:///<graph_name>/rel_card_payment.csv' AS row
MATCH (card:Card {card_id: row.card_id})
MATCH (payment:Payment {payment_id: row.payment_id})
MERGE (card)-[:AUTHORIZED]->(payment);

LOAD CSV WITH HEADERS FROM 'file:///<graph_name>/rel_payment_merchant.csv' AS row
MATCH (payment:Payment {payment_id: row.payment_id})
MATCH (merchant:Merchant {merchant_id: row.merchant_id})
MERGE (payment)-[:AT_MERCHANT]->(merchant);
