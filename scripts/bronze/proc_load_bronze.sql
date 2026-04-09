/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

create or alter procedure bronze.load_bronze as
begin
declare @starttime datetime ,@endtime datetime ,@batch_start_time datetime ,@batch_end_time datetime;
begin try

print '======================================================================';
print 'Loading data into bronze layer...';
print '======================================================================';

print'-----------------------------------------------------------------------';
print 'Loading CRM';
print'-----------------------------------------------------------------------';
	set @batch_start_time = getdate();
	set @starttime = getdate();
	print '>> Truncating Table : bronze.crm_cust_info'
	truncate table bronze.crm_cust_info;

	print '>> Inserting Data Into Table : bronze.crm_cust_info'
	bulk insert bronze.crm_cust_info
	from "C:\Users\Dnyanesh\Desktop\Data-warehouse\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv"
	with (
		fieldterminator = ',',
		firstrow = 2,
		tablock
	);
	set @endtime = getdate();
	print '>> Loding Duration : ' + cast(datediff(second,@starttime,@endtime) as nvarchar) + ' seconds';
	print'>>-------------------------|---------------------------------------------|--------------------';
	
	set @starttime = getdate();
	print '>> Truncating Table : bronze.crm_prd_info'
	truncate table bronze.crm_prd_info;

	print '>> Inserting Data Into Table : bronze.crm_prd_info'
	bulk insert bronze.crm_prd_info
	from "C:\Users\Dnyanesh\Desktop\Data-warehouse\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv"
	with (
		fieldterminator = ',',
		firstrow = 2,
		tablock
	);
	set @endtime=GETDATE();
	print '>> Loding Duration : ' + cast(datediff(second,@starttime,@endtime) as nvarchar) + ' seconds';
	print'>>-------------------------|---------------------------------------------|--------------------';
	
	
	set @starttime = getdate();
	print '>> Truncating Table : bronze.crm_sales_details'
	truncate table bronze.crm_sales_details;

	print '>> Inserting Data Into Table : bronze.crm_sales_details'
	bulk insert bronze.crm_sales_details
	from "C:\Users\Dnyanesh\Desktop\Data-warehouse\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv"
	with (
		fieldterminator = ',',
		firstrow = 2,
		tablock
	);
	set @endtime=GETDATE();
	print '>> Loding Duration : ' + cast(datediff(second,@starttime,@endtime) as nvarchar) + ' seconds';
	print'>>-------------------------|---------------------------------------------|--------------------';

print'-----------------------------------------------------------------------';
print 'Loading ERP';
print'-----------------------------------------------------------------------';
	set @starttime = getdate();
	print '>> Truncating Table : bronze.erp_cust_az12'
	truncate table bronze.erp_cust_az12;

	print '>> Inserting Data Into Table : bronze.erp_az12'
	bulk insert bronze.erp_cust_az12
	from "C:\Users\Dnyanesh\Desktop\Data-warehouse\sql-data-warehouse-project-main\datasets\source_erp\cust_az12.csv"
	with (
		fieldterminator = ',',
		firstrow = 2,
		tablock
	);
	set @endtime=GETDATE();
	print '>> Loding Duration : ' + cast(datediff(second,@starttime,@endtime) as nvarchar) + ' seconds';
	print'>>-------------------------|---------------------------------------------|--------------------';

	set @starttime = getdate();
	print '>> Truncating Table : bronze.erp_loc_a101'
	truncate table bronze.erp_loc_a101;

	print '>> Inserting Data Into Table : bronze.erp_cust_loc_a101'
	bulk insert bronze.erp_loc_a101
	from "C:\Users\Dnyanesh\Desktop\Data-warehouse\sql-data-warehouse-project-main\datasets\source_erp\loc_a101.csv"
	with (
		fieldterminator = ',',
		firstrow = 2,
		tablock
	);
	set @endtime=GETDATE();
	print '>> Loding Duration : ' + cast(datediff(second,@starttime,@endtime) as nvarchar) + ' seconds';
	print'>>-------------------------|---------------------------------------------|--------------------';
	
	
	set @starttime = getdate();
	print '>> Truncating Table : bronze.erp_px_cat_g1v2'
	truncate table bronze.erp_px_cat_g1v2;

	print '>> Inserting Data Into Table : bronze.erp_px_cat_g1v2'
	bulk insert bronze.erp_px_cat_g1v2
	from "C:\Users\Dnyanesh\Desktop\Data-warehouse\sql-data-warehouse-project-main\datasets\source_erp\px_cat_g1v2.csv"
	with (
		fieldterminator = ',',
		firstrow = 2,
		tablock
	);
	set @endtime=GETDATE();
	set @batch_end_time = getdate();
	print '>> Loding Duration : ' + cast(datediff(second,@starttime,@endtime) as nvarchar) + ' seconds';
	print'>>-------------------------|---------------------------------------------|--------------------';
	print '======================================================================';
	print 'Total Batch Duration : ' + cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar) + ' seconds';
	print '======================================================================';
	end try
	begin catch
	print '======================================================================';
	print 'Error Occured While Loading Data into Bronze Layer';
	print'Error Message'+error_message();
	print 'Error Number : ' + cast(error_number() as nvarchar);
	print 'Error Severity : ' + cast(error_severity() as nvarchar);
	print 'Error State : ' + cast(error_state() as nvarchar);
	print'=========================================================================';
	end catch
end