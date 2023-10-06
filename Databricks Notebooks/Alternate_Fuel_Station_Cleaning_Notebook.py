# Databricks notebook source
dbutils.fs.mount(source="wasbs://casestudygroup5@casestudystorage.blob.core.windows.net/", mount_point="/mnt/bronze", extra_configs = {"fs.azure.account.key.casestudystorage.blob.core.windows.net": "MlAPBstJ5N6ky5Vwg7jjX2Tp4LsX37g84lpJzfYTmBnFqH722wQnGL/Zi/y3bGyQfyMh5iH+I4d1+AStzbmFNw=="})

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Pre Processing

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/bronze/

# COMMAND ----------

df = spark.read.option("header", True).csv("dbfs:/mnt/bronze/raw/alt_fuel_stations.txt", sep="\t")

# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

df = df.filter(df["Fuel Type Code"].isin(["CNG", "BD", "ELEC", "E85","HY","RD","LPG","LNG"]))

# COMMAND ----------

display(df.select("Fuel Type Code").count())

# COMMAND ----------

for cols in df.columns:
    display(cols, df.filter(df[cols].isNull()).count())

# COMMAND ----------

df = df.drop("Street Address","Intersection Directions", "Plus4", "Station Phone", "EV Network Web", "Updated At", "Intersection Directions (French)" ,"Access Days Time (French)" ,"BD Blends (French)" ,"Groups With Access Code (French)", "EV Pricing (French)", "RD Blends (French)", "Federal Agency ID", "EV Other Info", "Federal Agency Name", "Federal Agency Code")

# COMMAND ----------

df.write.format("csv").mode("overwrite").option("header",True).option("path","dbfs:/mnt/bronze/STG").saveAsTable("AlternateFuelStation_stg")

# COMMAND ----------

display(df.groupBy("Country").count())

# COMMAND ----------

df = df.filter(df["Country"] == "US")

# COMMAND ----------

display(df.groupBy("Country").count())

# COMMAND ----------

df.write.format("csv").mode("overwrite").option("header",True).option("path","dbfs:/mnt/bronze/curated").saveAsTable("AlternateFuelStation_curated")

# COMMAND ----------

dbutils.fs.unmount("/mnt/bronze")
