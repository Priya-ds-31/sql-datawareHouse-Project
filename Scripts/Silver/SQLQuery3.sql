USE DataWarehouse;
SELECT *
FROM(
SELECT *,
row_number()over (partition by cst_id order by cst_create_date DESC) as flag_last
from bronze.crm_cust_info
)t WHERE flag_last = 1;

-- unwanted space
--expectation : no result

SELECT
   cst_firstname FROM bronze.crm_cust_info where cst_firstname != TRIM (cst_firstname);

SELECT
   cst_gndr FROM bronze.crm_cust_info where cst_gndr != TRIM (cst_gndr);

-- Data standardization & consistency
SELECT DISTINCT
   cst_gndr FROM bronze.crm_cust_info 

---silver check

SELECT
cst_id, count(*) from silver.crm_cust_info group by cst_id HAVING count(*)<1 or cst_id IS NULL;

SELECT
   cst_firstname FROM silver.crm_cust_info where cst_firstname != TRIM (cst_firstname);

SELECT
   cst_gndr FROM silver.crm_cust_info where cst_gndr != TRIM (cst_gndr);

SELECT DISTINCT
   cst_gndr FROM silver.crm_cust_info;

select
			prd_id,
			
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		FROM bronze.crm_prd_info;