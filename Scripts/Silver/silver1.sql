USE DataWarehouse;
SELECT
cst_id, count(*) from bronze.crm_cust_info group by cst_id HAVING count(*)<1 or cst_id IS NULL;

----

INSERT INTO silver.crm_cust_info (
			cst_id, 
			cst_key, 
			cst_firstname, 
			cst_lastname, 
			cst_marital_status, 
			cst_gndr,
			cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status, -- Normalize marital status values to readable format
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr, -- Normalize gender values to readable format
			cst_create_date
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t
		WHERE flag_last = 1; 
----
SELECT
cst_id, count(*) from silver.crm_cust_info group by cst_id HAVING count(*)<1 or cst_id IS NULL;

SELECT
   cst_firstname FROM silver.crm_cust_info where cst_firstname != TRIM (cst_firstname);

SELECT
   cst_gndr FROM silver.crm_cust_info where cst_gndr != TRIM (cst_gndr);

SELECT DISTINCT
   cst_gndr FROM silver.crm_cust_info;
   
SELECT * FROM silver.crm_cust_info;

----------- silver prd
INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
			prd_id,
		FROM bronze.crm_prd_info;
        