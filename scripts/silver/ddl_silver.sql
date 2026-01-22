/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/
	

drop table if exists silver.crm_customer_info;
				raise notice 'silver layer loding';
				
				create table silver.crm_customer_info(
				cst_id int,
				cst_key varchar(50),
				cst_firstname varchar(50),
				cst_lastname varchar(50),
				cst_marital_status varchar(50),
				cst_gndr varchar(50),
				cst_create_date date,
				dwh_created_date timestamp default current_timestamp
				);


drop table if exists silver.crm_prd_info;
					
					create table silver.crm_prd_info(
					prd_id int,
					cat_id varchar(50),
					prd_key varchar(40),
					prd_nm varchar(40),
					prd_cost int,
					prd_line varchar(40),
					prd_start_dt date,
					prd_end_dt date,
					dwh_created_date timestamp default current_timestamp
		
					);

drop table if exists silver.crm_sales_details;
		
		CREATE TABLE silver.crm_sales_details (
		    sls_order_num VARCHAR(40),
		    sls_prd_key VARCHAR(40),
		    sls_cust_id INT,
		    sls_order_dt date ,
		    sls_ship_dt date,
		    sls_due_dt date,
		    sls_sales INT,
		    sls_quantity INT,
		    sls_price INT,
			dwh_create_date timestamp default current_timestamp
		
		);

drop table if exists silver.erp_cust_a2z;
		
		create table silver.erp_cust_a2z(
		cid varchar(50),
		bdate date,
		gen varchar(50),
		dwh_created_date timestamp default current_timestamp
		
		);

drop table if exists silver.erp_loca101;

		create table silver.erp_loca101(
		cid varchar(50),
		cntry varchar(50),
		dwh_created_date timestamp default current_timestamp
		);


drop table if exists silver.erp_px_cat_giv;
		
		create table silver.erp_px_giv(
		id varchar(50),
		cat varchar(50),
		subcat varchar(50),
		maintenance varchar(50)
			);
