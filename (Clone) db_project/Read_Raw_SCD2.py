# Databricks notebook source
dbutils.widgets.text("TriggerName","Tr_sample_excel")
TriggerName = dbutils.widgets.get("TriggerName")

# COMMAND ----------

# DBTITLE 1,Import Lib


# COMMAND ----------

# DBTITLE 1,Common Functions
# MAGIC %run ./Common_functions

# COMMAND ----------

# DBTITLE 1,Read Config
jdbcUrl,connectionProperties = jdbc_connect_azure_sql()
pushdown_query = f"""(select a.trigger_name, b.*,c.job_dtls_id,c.dtl_key,c.dtl_value 
from dbo.tbl_trigger a 
join dbo.tbl_job b on a.trigger_id = b.trigger_id
join dbo.tbl_job_dtls c on b.jobid=c.jobid
where a.trigger_name = '{TriggerName}'
) test"""
df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
display(df)

# COMMAND ----------

# DBTITLE 1,getconfig
src_acc = df.filter(df.dtl_key == 'TargetAccount').collect()[0]['dtl_value']
src_dir = df.filter(df.dtl_key == 'tgt_dir').collect()[0]['dtl_value']
source_file =df.filter(df.dtl_key == 'Sourcefile').collect()[0]['dtl_value']

# COMMAND ----------

# DBTITLE 1,get config
file_loc=src_acc+src_dir+'/'+source_file
print(file_loc)
file_loc=file_loc.replace('://','://bronze@').replace('https:','abfss:').replace('.blob.','.dfs.').replace('.xlsx','.xlsx.parquet')
print(file_loc)

# COMMAND ----------

dbutils.fs.ls('/mnt')

# COMMAND ----------

#Copying PL_March.xlsx.parquet file to bronze container

dbutils.fs.cp('dbfs:/mnt/pldata/PL_March.xlsx.parquet','dbfs:/mnt/bronze/pldata/PL_March.xlsx.parquet')

# COMMAND ----------

# DBTITLE 1,Read Data
df=spark.read.parquet('dbfs:/mnt/bronze/pldata/PL_March.xlsx.parquet')

# COMMAND ----------

# As the header is missed, this code extracts the first row as header

# Extract the first row as the header
header_row = df.first()

# Define new column names based on the first row
new_column_names = list(header_row.asDict().values())

# Remove the header row and rename the columns
df = df.filter(df["Prop_0"] != "product_id")  # Exclude the original header row
df = df.toDF(*new_column_names)

# Display the updated DataFrame
df.display()


# COMMAND ----------

# DBTITLE 1,Display
display(df)

# COMMAND ----------

# DBTITLE 1,Drop Duplicate
#df=df.dropDuplicates()

# COMMAND ----------

# DBTITLE 1,add a new column
from  pyspark.sql.functions import *
from pyspark.sql.types import TimestampType

df=df.withColumn("filename", input_file_name())\
.withColumn("is_active",lit(1))\
.withColumn("effective_start",current_timestamp())\
.withColumn('effective_end', lit(None).cast(TimestampType()))


# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,de dupe based on OrderID
duplicated_df = df.dropDuplicates(["Sku"])

# COMMAND ----------

display(duplicated_df)

# COMMAND ----------

# DBTITLE 1,Save to a delta table using scd-2

dbfs_path = "dbfs:/mnt/pl_march_sales_report"
if not spark.catalog.tableExists( "pl_march_sales_report"):
  print("Table not exists")
  duplicated_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("pl_march_sales_report")
else:
  # print("Table exist")
  deduplicated_df.createOrReplaceTempView("temp_pl_march_sales_report")

  merge_query = """
  MERGE INTO pl_march_sales_report AS c USING (
  SELECT    -- UPDATES
    ur.Sku as merge_key,
    ur.*
  FROM
    temp_pl_march_sales_report ur
  UNION ALL
  SELECT    -- INSERTS
    NULL as merge_key,
    ur.*
  FROM
    temp_pl_march_sales_report ur
    JOIN pl_march_sales_report c ON c.Sku = ur.Sku
    AND c.is_active = 1
  WHERE -- ignore records with no changes
       c.Style_Id<>ur.Style_Id
      or c.Catalog<>ur.Catalog
      or c.Category<>ur.Category
      or c.Weight<>ur.Weight
      or c.TP_1<>ur.TP_1
      or c.TP_2<>ur.TP_2
      or c.MRP_Old<>ur.MRP_Old
      or c.Final_MRP_Old<>ur.Final_MRP_Old
      or c.Ajio_MRP<>ur.Ajio_MRP
      or c.Amazon_MRP<>ur.Amazon_MRP
      or c.Amazon_FBA_MRP<>ur.Amazon_FBA_MRP
      or c.Flipkart_MRP<>ur.Flipkart_MRP
      or c.Limeroad_MRP<>ur.Limeroad_MRP
      or c.Myntra_MRP<>ur.Myntra_MRP
      or c.Paytm_MRP<>ur.Paytm_MRP
      or c.Snapdeal_MRP<>ur.Snapdeal_MRP

    
) u ON c.Sku = u.merge_key -- Match record condition
WHEN MATCHED AND -- ignore records with no changes
       c.Style_Id<>u.Style_Id
      or c.Catalog<>u.Catalog
      or c.Category<>u.Category
      or c.Weight<>u.Weight
      or c.TP_1<>u.TP_1
      or c.TP_2<>u.TP_2
      or c.MRP_Old<>u.MRP_Old
      or c.Final_MRP_Old<>u.Final_MRP_Old
      or c.Ajio_MRP<>u.Ajio_MRP
      or c.Amazon_MRP<>u.Amazon_MRP
      or c.Amazon_FBA_MRP<>u.Amazon_FBA_MRP
      or c.Flipkart_MRP<>u.Flipkart_MRP
      or c.Limeroad_MRP<>u.Limeroad_MRP
      or c.Myntra_MRP<>u.Myntra_MRP
      or c.Paytm_MRP<>u.Paytm_MRP
      or c.Snapdeal_MRP<>u.Snapdeal_MRP

  THEN
  UPDATE SET -- Update fields on 'old' records
      is_active = 0,
      effective_end = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT
    *

  """

  spark.sql(merge_query)



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pl_march_sales_report
