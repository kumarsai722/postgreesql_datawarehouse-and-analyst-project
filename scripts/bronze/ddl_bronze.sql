/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/





	--CREATING A DATABASE
		--create database datawarehouse_1;
		
		--CREATE SCHMEAS TO STORE DIFFRENT TYPES OF DATA LIKE BRONZE,SILVER,GOLD..

			
			--CREATING SCHEMAS
			--create schema bronze;
			--create schema silver;
			--create schema gold;
		
		
		--UNDERSTAND THE DATA FROM SILVER FILES ..
		
		/*  1)WE HAVE TWO FOLDER NAMED source_crm and source_erp
		Now RETRIVE THE DATA FROMTHESE TWO FOLDER..*/


		drop table if exists bronze.crm_customer_info;



		create table bronze.crm_customer_info(
		cst_id int,
		cst_key varchar(50),
		cst_firstname varchar(50),
		cst_lastname varchar(50),
		cst_marital_status varchar(50),
		cst_gndr varchar(50),
		cst_create_date date
		);
		
		--copying data from cust_info
		copy bronze.crm_customer_info
		FROM 'C:\pg_admin_folder\cust_info.csv'
		delimiter ','
		csv 
		header
		null '';
		
		--select count(*) from  bronze.crm_customer_info
		--select * from bronze.crm_customer_info;
		
		/********************************************************************* */
		
		-- 2)CREATING CRM_PROD_INFO TABLE 


		drop table if exists  bronze.crm_prd_info;

		CREATE table bronze.crm_prd_info(
		prd_id int,
		prd_key varchar(50),
		prd_nm varchar(50),
		prd_cost int,
		prd_line varchar(50),
		prd_start_dt date,
		prd_end_dt date
		);
		
		
		--	COPY DATA FROM CSV FILES
		copy bronze.crm_prd_info
		FROM 'C:\pg_admin_folder\prd_info.csv'
		delimiter ','
		csv 
		header
		null '';
		
		
		--select * from bronze.crm_prd_info;
		
		
		/********************************************************/
		
		-- 3) CREATING CRM_sales_details

		drop table if exists  bronze.crm_sales_details;

		create table bronze.crm_sales_details(
		sls_order_num varchar(50),
		sls_prd_key varchar(50),
		sls_cust_id int,
		sls_order_dt int,
		sls_ship_dt int,
		sls_due_dt int,
		sls_sales int,
		sls_quantity int,
		sls_price int
		);
		
		
		--COPY DATA FROM CSV FILES..
		copy  bronze.crm_sales_details
		from 'C:\pg_admin_folder\sales_details.csv'
		DELIMITER ','
		CSV 
		HEADER 
		NULL '';
		
		
		--select *from bronze.crm_sales_details;
		/**********************************************************/
		drop table if exists bronze.erp_cust_a2z ;

		create table bronze.erp_cust_a2z(
		cid varchar(50),
		bdate date,
		gen varchar(50)
		);
		
		
		--COPY DATA FROM CSV FILES..
		copy  bronze.erp_cust_a2z
		from 'C:\pg_admin_folder\cust_a2z.csv'
		DELIMITER ','
		CSV 
		HEADER 
		NULL '';
		
		
		
		--select *from bronze.erp_cust_a2z;
		
		/***********************************
		**********************************/

		drop table if exists bronze.erp_loca101 ;

		
		create table  bronze.erp_loca101
		(
		cid varchar(50),
		cntry varchar(50)
		);
		
		--select * from  bronze.erp_loca101;
		
		--copying data from csv files....
		copy  bronze.erp_loca101
		from 'C:\pg_admin_folder\loca_101.csv'
		DELIMITER ','
		CSV 
		HEADER 
		NULL '';
		
		/***********************************************
		***********************************************/
		--CRAETING TABLE PX_GIV 

		drop table if exists bronze.erp_px_giv ;

		create table bronze.erp_px_giv(
		id varchar(50),
		cat varchar(50),
		subcat varchar(50),
		maintenance varchar(50)
		);
		
		--select * from bronze.erp_px_giv
		--COPYING DATA FROM CSV FILES
		copy  bronze.erp_px_giv
		from 'C:\pg_admin_folder\px_giv.csv'
		DELIMITER ','
		CSV 
		HEADER 


