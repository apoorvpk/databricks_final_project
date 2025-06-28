# Databricks notebook source
import requests
import json

#sample 1 
# url = "https://vpic.nhtsa.dot.gov/api/vehicles/getallmanufacturers?format=json"

#sample 2
url = "https://api.restful-api.dev/objects"

payload = {}
headers = {'Content-Type': 'application/json'}

response = requests.request("GET", url, headers=headers, data=payload)

# print(response.text)


# COMMAND ----------

df = spark.read.json(sc.parallelize([response.text]))
df.createOrReplaceTempView("vw_temp_api")

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.select("id","name"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_temp_api

# COMMAND ----------

# MAGIC %sql
# MAGIC select id,name,data.* from vw_temp_api

# COMMAND ----------

df1 = spark.sql("select id, name, data.* from vw_temp_api")

# COMMAND ----------

df_cols = [col.replace(' ', '_').lower() for col in df1.columns]
duplicate_col_index = [idx for idx, val in enumerate(df_cols) if val in df_cols[:idx]]
for i in duplicate_col_index:
  df_cols[i] = df_cols[i] + '_'+ str(i)
df1 = df1.toDF(*df_cols)

# COMMAND ----------

display(df1)

# COMMAND ----------

#saving to a delta table
spark.sql("DROP TABLE IF EXISTS inv_data")
df1.write.saveAsTable('inv_data')
