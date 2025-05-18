/* 
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

*/
  
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME;
	BEGIN TRY
		PRINT('===================================================');
		PRINT('Loading Bronze Layer');
		PRINT('===================================================');

		PRINT('----------------------------------------------------');
		PRINT('Loading CRM Tables');
		PRINT('----------------------------------------------------');

		SET @start_time = GETDATE() --beginning time
		PRINT('Truncating table : bronze.crm_cust_info');
		TRUNCATE TABLE bronze.crm_cust_info; --make the table empty first
		PRINT('Inserting Data Into: bronze.crm_cust_info');
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\DELL\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, --fisrtrow is actually the seconderow
			FIELDTERMINATOR = ',', --Separator
			TABLOCK
		);
		SET @end_time = GETDATE(); --ending time
		PRINT('>>Load duration : ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + 'seconds');


		PRINT('Truncating table : bronze.crm_prd_info');
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT('Inserting Data Into: bronze.crm_prd_info');
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\DELL\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		PRINT('Truncating table : bronze.crm_sales_details');
		TRUNCATE TABLE bronze.crm_sales_details; 
		PRINT('Inserting Data Into: bronze.crm_sales_details');
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\DELL\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		PRINT('----------------------------------------------------');
		PRINT('Loading ERP Tables');
		PRINT('----------------------------------------------------');

		PRINT('Truncating table : bronze.erp_cust_az12');
		TRUNCATE TABLE bronze.erp_cust_az12; 
		PRINT('Inserting Data Into :bronze.erp_cust_az12');
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\DELL\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT('Truncating table : bronze.erp_loc_a101');
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT('Inserting Data Into :bronze.erp_loc_a101');
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\DELL\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT('Truncating table : bronze.erp_px_cat_g1v2');
		TRUNCATE TABLE bronze.erp_px_cat_g1v2; 
		PRINT('Inserting Data Into :bronze.erp_px_cat_g1v2');
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\DELL\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
	END TRY
	BEGIN CATCH 
		PRINT('ERROR OCCURED DURING LOADING');
	END CATCH
END
