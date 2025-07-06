# Databricks notebook source
dbutils.widgets.text("TriggerName","Tr_sample_csv")
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

dbutils.secrets.listScopes()

# COMMAND ----------

# DBTITLE 1,getconfig
src_acc = df.filter(df.dtl_key == 'TargetAccount').collect()[0]['dtl_value']
src_dir = df.filter(df.dtl_key == 'tgt_dir').collect()[0]['dtl_value']
source_file =df.filter(df.dtl_key == 'Sourcefile').collect()[0]['dtl_value']

print(src_acc)
print(src_dir)
print(source_file)

# COMMAND ----------

# DBTITLE 1,get config
file_loc=src_acc+src_dir+'/'+source_file
print(file_loc)
file_loc=file_loc.replace('://','://bronze@').replace('https:','abfss:').replace('.blob.','.dfs.').replace('.csv','.parquet')
print(file_loc)

# COMMAND ----------

#setting te configuration to connect to my adls account

spark.conf.set(
  "fs.azure.account.key.adlsprojectazderaw.dfs.core.windows.net",
  dbutils.secrets.get(scope="sqlserver", key="storageaccountaccesskey")
)







# COMMAND ----------

df=spark.read.parquet(file_loc)

# COMMAND ----------

#testing
dbutils.fs.mkdirs("/mnt/pldata/salesdata")


# COMMAND ----------

# MAGIC %fs ls "/mnt"

# COMMAND ----------

dbutils.fs.mounts("/mnt/salesdata")

# COMMAND ----------

#For unmount
dbutils.fs.unmount("/mnt/pldata")

# COMMAND ----------

#Copying Amazon_Sale_Report.parquet file to bronze container
dbutils.fs.ls('/mnt')
dbutils.fs.cp('dbfs:/mnt/salesdata/Amazon_Sale_Report.parquet','dbfs:/mnt/bronze/salesdata/Amazon_Sale_Report.parquet')


# COMMAND ----------

df = spark.read.parquet('dbfs:/mnt/bronze/salesdata/Amazon_Sale_Report.parquet')

# COMMAND ----------

# DBTITLE 1,Display
display(df)

# COMMAND ----------

# DBTITLE 1,add a new column
from  pyspark.sql.functions import input_file_name

df=df.withColumn("filename", input_file_name())

# COMMAND ----------

df.display()

# COMMAND ----------

# DBTITLE 1,de dupe based on OrderID
df = df.dropDuplicates(["order_id"])

# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,Save to a delta table using scd-1
dbfs_path = "dbfs:/mnt/amazon_sales_report"

if not spark.catalog.tableExists("amazon_sales_report"):
  print("Table not exists")
  df.write.format("delta").mode("overwrite").saveAsTable("amazon_sales_report")
else:
  print("Table exist")
  df.createOrReplaceTempView("temp_amazon_sales_report")

  merge_query = """
  MERGE INTO amazon_sales_report AS target
  USING temp_amazon_sales_report AS source
  ON target.OrderID = source.OrderID
  WHEN MATCHED THEN
    UPDATE SET *
  WHEN NOT MATCHED THEN
    INSERT *
  """

  spark.sql(merge_query)



# COMMAND ----------

df3 = spark.table("amazon_sales_report")
display(df3)

# COMMAND ----------

# Save as Parquet
df3.write.mode("overwrite").parquet("/mnt/databricksoutput/SCD1/Parquet")

# COMMAND ----------

# Or save as Delta
df3.write.format("delta").mode("overwrite").save("/mnt/databricksoutput/SCD1/Delta")
