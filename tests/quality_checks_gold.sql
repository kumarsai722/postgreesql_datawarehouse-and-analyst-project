/*
===============================================================================
Quality Checks in gold_layer 
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results 
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;


--CHECKING DATA INTEGRATION 
After joining we have two columns named cst_gndr from customer_info and another from caz.gen fromcus_a2z table
..now check any distinct between then or any change b/w these two columns..

select  distinct cst_gndr,caz.gen
from silver.crm_customer_info as ci
left join silver.erp_cust_a2z as caz
ON ci.cst_key=caz.cid
left join  silver.erp_loca101 as lo
ON ci.cst_key=lo.cid;
IT HAVE DISTINCT VALUES AND DIFFERENT VALUES FROM BOTH TABLES..

SO TAKE FIRST MASTER TABLE AS MAIN DATA IF NOT FOUND IN MASTER THEN TAKE FROM 2ND TABLE...


--SOLUTION FOR IT
select  distinct cst_gndr,caz.gen,
case when ci.cst_gndr !='n/a' then ci.cst_gndr
     else coalesce(caz.gen,'n/a')
end as gender
from silver.crm_customer_info as ci
left join silver.erp_cust_a2z as caz
ON ci.cst_key=caz.cid
left join  silver.erp_loca101 as lo
ON ci.cst_key=lo.cid;


-- ====================================================================
-- Checking 'gold.product_key'
-- ====================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL  

------foregin_key integrity....
select *
from gold.fact_sales as sd
left join gold.dim_prdocuts as dp
ON sd.product_key=dp.product_key
left join gold.dim_customers as dc
ON sd.customer_key =dc.customer_key
where sd.product_key is null;
