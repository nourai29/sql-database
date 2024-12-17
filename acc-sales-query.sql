WITH BASE AS(
       (SELECT transaction_id, 
               PARSE_DATE('%Y%m%d', date) AS date,
               product_sku,
               'Online' AS sales_medium,
               CAST(REPLACE(quantity, 'NA', '0')  AS INT64) AS quantity,
               price
       FROM `acc-383113.marketing.sales_online`)
       UNION ALL
       (SELECT CAST(invoice_no AS INT64) AS transaction_id,
               CAST(invoice_date AS DATE) AS date,
               product_sku,
               'Offline' AS sales_medium,
               CAST(R.quantity AS INT64) AS quantity,
               avg_price * CAST(R.quantity AS INT64) AS price,
       FROM `acc-383113.marketing.sales_retail` R
       LEFT JOIN `acc-383113.marketing.catalog` C USING(stock_code)
       LEFT JOIN (SELECT DISTINCT AVG(avg_price) AS avg_price, product_sku
           FROM `acc-383113.marketing.sales_online`
           GROUP BY 2) O USING(product_sku))
               ) 
 SELECT B.date,
       SUM(IF(sales_medium = 'Online', orders, NULL)) AS online_orders,
       SUM(IF(sales_medium = 'Online', units, NULL)) AS online_units,
       SUM(IF(sales_medium = 'Online', gmv, NULL)) AS online_gmv,
       SUM(IF(sales_medium = 'Offline', orders, NULL)) AS offline_orders,
       SUM(IF(sales_medium = 'Offline', units, NULL)) AS offline_units,
       SUM(IF(sales_medium = 'Offline', gmv, NULL)) AS offline_gmv,
       online_cost, offline_cost
       FROM (SELECT date, 
             sales_medium, 
             SUM(price) AS gmv, 
             COUNT(DISTINCT transaction_id) AS orders, 
             SUM(quantity) AS units
            FROM BASE
            GROUP BY 1,2) B
 LEFT JOIN (SELECT PARSE_DATE('%d/%m/%Y', date) AS date,
               CAST(online_cost AS FLOAT64) AS online_cost,
               CAST(offline_cost AS FLOAT64) AS offline_cost
       FROM `acc-383113.marketing.mktg_cost`) M ON B.date = M.date 
 GROUP BY 1,8,9
