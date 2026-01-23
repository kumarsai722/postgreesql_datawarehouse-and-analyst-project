/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =======================================================================


--1)dim_customers table
drop view if exists gold.dim_customers;
  
create view gold.dim_customers as 
		select
		row_number() over(order by ci.cst_id) as customer_key,
		ci.cst_id as customer_id,
		ci.cst_key as  customer_number,
		ci.cst_firstname as firstname,
		ci.cst_lastname as lastname,
		case when ci.cst_gndr !='n/a' then ci.cst_gndr
		     else coalesce(caz.gen,'n/a')
		end as gender,
		caz.bdate as  birthdate,
		ci.cst_marital_status as marital_status,
		lo.cntry as  country,
		ci.cst_create_date as create_date
		
		from  silver.crm_customer_info as ci
		left join silver.erp_cust_a2z as caz
		ON ci.cst_key=caz.cid
		left join  silver.erp_loca101 as lo
		ON ci.cst_key=lo.cid;




-- =============================================================================
-- Create Dimension: gold.dim_cproducts
-- =======================================================================
--2)gold.dim_products table
drop view if exists gold.dim_prdocuts;
create view gold.dim_prdocuts as 
		select 
		row_number() over(order by pi.prd_start_dt,pi.prd_key) as product_key,
		pi.prd_id as product_id,
		pi.prd_key as product_number,
		pi.prd_nm as product_name,
		pi.cat_id as  category_id,
		pd.cat as category,
		pd.subcat as subcategory,
		pd.maintenance,
		pi.prd_cost as cost,
		pi.prd_line as product_line,
		pi.prd_start_dt as start_date 
		
		from silver.crm_prd_info as pi
		left join silver.erp_px_giv as pd
		ON pi.cat_id=pd.id
		where pi.prd_end_dt is null;




-- =============================================================================
-- Create fcat_table: gold.fcat_sales
-- =======================================================================
--3)gold.fact_sales

drop view if exists gold.fact_sales;
create view gold.fact_sales as 
select sls_order_num,
dp.product_key,
dc.customer_key,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from silver.crm_sales_details as sd
left join gold.dim_prdocuts as dp
ON sd.sls_prd_key=dp.product_number
left join gold.dim_customers as dc
ON sd.sls_cust_id =dc.customer_id;

--
