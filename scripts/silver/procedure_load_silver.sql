/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/



--===========================================================================================
--===========================================================================================
/*               SILVER LAYER     */

--===========================================================================================
--===========================================================================================


create or replace procedure silver.load_silver()
language plpgsql
as $$
begin
		
		--INSERTING INTO SILVER LAYER AFTER CLEANING THE DATA..
				raise notice'INSERTING DATA FROM BROZE TO SILVER';
				
				drop table if exists silver.crm_customer_info;
				raise notice 'silver layer loding';
				
				
		
		INSERT INTO silver.crm_customer_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)
		select cst_id,
		cst_key,
		case when lower(cst_firstname) != trim(lower(cst_firstname)) then trim(cst_firstname)
				else cst_firstname
		end as cst_firstname,
		case when lower(cst_lastname) != trim(lower(cst_lastname)) then trim(cst_lastname)
				else cst_lastname
		end as cst_lastname,
		case when trim(upper(cst_marital_status)) ='M' then 'Married'
			 when trim(upper(cst_marital_status))='S' then 'Single'
			 else 'n/a'
		end as cst_marital_status,
		case when trim(upper(cst_gndr)) ='M' then 'Male'
			 when trim(upper(cst_gndr))='F' then 'Female'
			 else 'n/a'
		end as gender,
		cst_create_date
		--dwh_create_date timestamp default current_timestamp
		from(
		select *,
		row_number() over(partition by cst_id order by cst_create_date desc ) as row
		from bronze.crm_customer_info 
		where cst_id is  not null)t
		where t.row=1;


		/*----------------------------------------
		FINDING DUPLICATES
		----------------------------------------*/
		
		
		/*select * from silver.crm_customer_info
		--cheking for nulls or duplicates in P_KEY.. in bronze.crm_customer_info....
		
		select * from bronze.crm_customer_info;
		
		select cst_id,count(cst_id) from silver.crm_customer_info
		group by cst_id having count(cst_id)>1;
		
		select distinct cst_key from bronze.crm_customer_info
		where cst_key NOT like 'A%'
		--CHECK FOR UNWANTED SPACES IN NAMES
		
		select cst_firstname,cst_lastname from silver.crm_customer_info
		where cst_firstname !=trim(cst_firstname) and cst_lastname!=trim(cst_lastname)
		
		--CHECK THE CONSISTENCY OF VALUES IN LOW CORDINALITY COLUMNS..
		select cst_mar,distinct count(cst_marital_status) from silver.crm_customer_info
		
		----------------------------------------
		----------------------------------------*/
		/*
		select cst_id,
		cst_key,
		case when lower(cst_firstname) != trim(lower(cst_firstname)) then trim(cst_firstname)
				else cst_firstname
		end as cst_firstname,
		case when lower(cst_lastname) != trim(lower(cst_lastname)) then trim(cst_lastname)
				else cst_lastname
		end as cst_lastname,
		case when trim(upper(cst_marital_status)) ='M' then 'Married'
			 when trim(upper(cst_marital_status))='S' then 'Single'
			 else 'n/a'
		end as cst_marital_status,
		case when trim(upper(cst_gndr)) ='M' then 'Male'
			 when trim(upper(cst_gndr))='F' then 'Female'
			 else 'n/a'
		end as gender,
		cst_create_date
		--dwh_create_date timestamp default current_timestamp
		from(
		select *,
		row_number() over(partition by cst_id order by cst_create_date desc ) as row
		from bronze.crm_customer_info 
		where cst_id is  not null)t
		where t.row=1;
		
		*/
		
		/*--------------------------------------------------------------------------------
			**CRM_PRODUCT_TABLE ***** FINDING DUPLICATES NULLS AND INSETING INTO SILVER 
			------------------------------------------------------ */
		
			
			
			/*--CHEKING DUPLICATE P_KEY VALES I.E prd_id 
			select prd_id,count(*) from silver.crm_prd_info group by prd_id
			having count(*)>1 or prd_id is null;
			
			select prd_key,count(*) from bronze.crm_prd_info group by prd_key
			having count(*)>1 or prd_key is null;
			
			select prd_cost from silver.crm_prd_info
			where prd_cost<0 or prd_cost is null;
			
			select distinct(prd_line) from  silver.crm_prd_info;
			
			select *from silver.crm_prd_info where prd_nm !=trim(prd_nm)
			
			
			select * from silver.crm_prd_info 
			where prd_start_dt<prd_end_dt;
			
			select prd_cost from bronze.crm_prd_info  where prd_cost<0 or prd_cost is null;  */
			
			/*-----------------------------------------------------
			------------------------------------------------------
			*************INSERTING DATA INTO SILVER LAYER**********
			______________________________________________________
			--------------------------------------------------------*/
		
			
					
		
					--select *from silver.crm_prd_info
					
		
			
			INSERT INTO silver.crm_prd_info(
			prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt
			)
			select 
			prd_id,
			replace(substring(prd_key,1,5),'-','_')as cat_id,
			substring(prd_key from 7 for length(prd_key)) as prd_key,
			prd_nm,
			coalesce(prd_cost,0)as prd_cost,
			
			case when upper(trim(prd_line))='M' then 'Mountain'
				when upper(trim(prd_line))='S' then 'Other Sales'
				when upper(trim(prd_line))='R' then 'Road'
				when upper(trim(prd_line))='T' then 'Touring'
				else 'n/a'
			end as prd_line,
			 prd_start_dt,
			lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as prd_end_dt
			from bronze.crm_prd_info;
		
			--select * from silver.crm_prd_info
		
		
		/*============================================================================
		3) CRM_SALES_DETAILS
		INSERTING DATA FROM BRONZE TO SILVER AFTER CLEANING 
		============================================================================*/
		
		--CONVERTIN INT TO DATE (20121004)
		/*SELECT sls_order_dt,
		TO_DATE(sls_order_dt::text, 'YYYY-MM-DD')
		FROM bronze.crm_sales_details;
		
		select sls_cust_id,count(*) from bronze.crm_sales_details
		group by sls_cust_id having count(*)!=1;
		
		*/
		
		
		
		
		select *from bronze.crm_sales_details;
		
			INSERT INTO silver.crm_sales_details(
			sls_order_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price)
			
			select 
			sls_order_num,
			sls_prd_key,
			sls_cust_id,
			case when sls_order_dt=0 or length(sls_order_dt::text)!=8 then null
				else
				TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
				end AS sls_order_dt,
			case when sls_ship_dt =0 or length(sls_ship_dt::text)!=8 then null
				else 
				TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
				end AS sls_ship_dt,
			
			case when sls_due_dt=0 or length(sls_due_dt::text)!=8 then null
				else
				TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
			end AS sls_due_dt,
			case when sls_sales is null or sls_sales <=0 or sls_sales !=sls_quantity*abs(sls_price)
				then sls_quantity*abs(sls_price)
				else sls_sales
				end as sls_sales,
			sls_quantity,
			case when sls_price is null or sls_price<=0 then sls_price/coalesce(sls_quantity,0)
			else sls_price
			end as sls_price
			from bronze.crm_sales_details;
		
		
		/*===========================================================================================
		========================================================================================== */
		/*    ERP TABLES INSERTING INTO SILVER LAYER */
		
		/*select *from bronze.erp_cust_a2z;
		--FINDING DISTINCT VALUES FROM THE CID ADN GENDER.......
		
		select cid from silver.erp_cust_a2z where cid like 'NAS%';
		
		SELECT distinct gen from silver.erp_cust_a2z;
		
		--CHECK IF BDATE IS GRETER THAN CURRENDATE OR BDATE IS NULL
		select bdate from silver.erp_cust_a2z where bdate >current_timestamp

		*/
		
		
		INSERT into silver.erp_cust_a2z(
		cid,bdate,gen
		)
		SELECT 
		case when cid like 'NAS%' then substring(cid,4,length(cid))
		else cid
		end as cid,
		case when bdate>current_timestamp then null
		else bdate
		end as bdate,
		
		case when upper(trim(gen)) in ('M','MALE') then 'Male'
			 when upper(trim(gen)) in('F','FEMALE') then 'Female'
			 else 'n/a'
		end as gen
		from bronze.erp_cust_a2z;
		
		--select *from bronze.erp_cust_a2z;
		
		
		/*
		==========================================================================================
		
		==========================================================================================
		*/
		--5)	********ERP_LOC101____TABLE****************
		
		/*select * from bronze.erp_loca101;
		
		select cid,count(*) from bronze.erp_loca101
		group by cid having count(*)>1; */



		INSERT INTO silver.erp_loca101(cid,cntry)
			select
			case when cid like 'AW%' then replace(cid,'-','')
			else cid
			end as cid,
			case when trim(cntry) ='DE' then 'Germany'
				when trim(cntry) in('US','USA') then 'United States'
				when trim(cntry)='' or cntry is null then 'n/a'
				else trim(cntry)
			end as cntry
			from bronze.erp_loca101;
		
		
		--select * from bronze.erp_loca101;
		
		/*==========================================================================================
		
		==========================================================================================*/
		
		--6)	********ERP_PX_GIV____TABLE****************
		
		--select *from silver.erp_px_giv;
		
		
			
			INSERT INTO silver.erp_px_giv
			select id,
			cat,
			subcat,
			maintenance 
			from  bronze.erp_px_giv;

			raise notice 'silver layer laoded';
end;
$$;

call silver.load_silver();
