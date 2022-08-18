# Databricks notebook source
# MAGIC %scala
# MAGIC val tags = com.databricks.logging.AttributionContext.current.tags
# MAGIC val name = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
# MAGIC val username = if (name != "unknown") name else dbutils.widgets.get("databricksUsername")
# MAGIC 
# MAGIC val userhome = s"dbfs:/user/$username"
# MAGIC 
# MAGIC // Set the user's name and home directory
# MAGIC spark.conf.set("com.databricks.mounts.username", username)
# MAGIC spark.conf.set("com.databricks.mounts.userhome", userhome)

# COMMAND ----------

home_dir = spark.conf.get("com.databricks.mounts.userhome")

source_db = 'cdc_test_foundation'
target_db = 'cdc_test_trusted'
agg_db = "cdc_test_unified"

source_table = 'inp_table'
target_table = 'merge_table'
target_table_full = target_table + "_full"
agg_table = "aggregate_table"

source_loc = "{}/{}/{}".format(home_dir, source_db, source_table)
target_loc = "{}/{}/{}".format(home_dir, target_db, target_table)
target_loc_full = target_loc + "_full"
agg_loc = "{}/{}/{}".format(home_dir, agg_db, agg_table)

# COMMAND ----------

spark.sql("DROP DATABASE {} CASCADE".format(source_db))
spark.sql("DROP DATABASE {} CASCADE".format(target_db))
spark.sql("DROP DATABASE {} CASCADE".format(agg_db))
dbutils.fs.rm("{}/{}".format(home_dir, source_db), True)
dbutils.fs.rm("{}/{}".format(home_dir, target_db), True)
dbutils.fs.rm("{}/{}".format(home_dir, agg_db), True)
dbutils.fs.ls(home_dir)

# COMMAND ----------

import pyspark.sql

row_type = pyspark.sql.Row('key', 'category', 'val', 'timestamp', 'part', 'deleted')
curr_time = 1
in_df = spark.createDataFrame([
    row_type('key1', 'cat1', 1, curr_time, 'part1', ''),
    row_type('key2', 'cat1', 2, curr_time, 'part1', ''),
    row_type('key3', 'cat2', 3, curr_time, 'part2', ''),
    row_type('key4', 'cat2', 4, curr_time, 'part2', ''),
    row_type('key5', 'cat3', 5, curr_time, 'part3', ''),
    row_type('key6', 'cat3', 6, curr_time, 'part3', ''),
])

spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(source_db))
spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(target_db))
# in_df.write.mode("overwrite").format("delta").partitionBy('part').save(source_loc)
in_df.write.mode("overwrite").format("delta").save(source_loc)
spark.sql("CREATE TABLE IF NOT EXISTS {}.{} USING DELTA LOCATION '{}'".format(source_db, source_table, source_loc))
spark.sql("ALTER TABLE {}.{} SET TBLPROPERTIES ('auto.purge'='true')".format(source_db, source_table))
display(in_df)

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS {}.{} USING DELTA LOCATION '{}' AS SELECT * FROM {}.{}".format(target_db, target_table_full, target_loc_full, source_db, source_table))
spark.sql("ALTER TABLE {}.{} SET TBLPROPERTIES ('auto.purge'='true')".format(target_db, target_table_full))

# COMMAND ----------

spark.sql("CREATE OR REPLACE VIEW {db}.{} AS SELECT key, category, val, part FROM {db}.{} WHERE deleted == ''".format(target_table, target_table_full, db=target_db))

# COMMAND ----------

display(spark.sql("SELECT * FROM {}.{} LIMIT 10".format(target_db, target_table)))

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(agg_db))
spark.sql("CREATE TABLE IF NOT EXISTS {}.{} USING DELTA LOCATION '{}' AS SELECT category, SUM(val) as sum FROM {}.{} GROUP BY category".format(agg_db, agg_table, agg_loc, target_db, target_table))
spark.sql("ALTER TABLE {}.{} SET TBLPROPERTIES ('auto.purge'='true')".format(agg_db, agg_table))
display(spark.sql("SELECT * FROM {}.{}".format(agg_db, agg_table)))

# COMMAND ----------

for loc in [source_db, target_db, agg_db]:
  print(dbutils.fs.ls("{}/{}".format(home_dir, loc)))

# COMMAND ----------

display(spark.sql("DESCRIBE EXTENDED {}.{}".format(target_db, target_table)))

# COMMAND ----------

# This is not working with the target_db.target_table view as intended. Error message: must be delta table. Is there a way to create a view that has delta structure enabled?
# There is no specific information on this in the docs, views appear to support only standard table transformations, even when derived from delta tables
# https://docs.databricks.com/delta/delta-batch.html#views-on-tables

# From: https://docs.databricks.com/delta/delta-update.html#writing-streaming-aggregates-in-update-mode-using-merge-and-foreachbatch-notebook

def upsertToDelta(microBatchOutputDF, batchId): 
  # Set the dataframe to view name
  microBatchOutputDF.createOrReplaceTempView("updates")

  # ==============================
  # Supported in DBR 5.5 and above
  # ==============================

  # Use the view name to apply MERGE
  # NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe
  microBatchOutputDF._jdf.sparkSession().sql("""
      MERGE INTO {}.{} t
      USING updates s
      ON s.category = t.category
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
  """.format(agg_db, agg_table))

# # Reset the output aggregates table
# spark.createDataFrame([ Row(key=0, count=0) ]).write \
#   .format("delta").mode("overwrite").saveAsTable("aggregates")

# Define the aggregation
agg_df = spark.readStream \
    .format("delta") \
    .table("{}.{}".format(target_db, target_table_full)) \
    .select('category', 'val') \
    .where("deleted == ''") \
    .groupBy('category') \
    .agg({'val': 'sum'}) \
    .withColumnRenamed('SUM(val)', 'sum')

# From: https://databricks.com/blog/2017/05/22/running-streaming-jobs-day-10x-cost-savings.html
# Start the query to upsert once into aggregates tables in update mode
agg_df.writeStream \
    .trigger(once=True) \
    .format("delta") \
    .foreachBatch(upsertToDelta) \
    .outputMode("update") \
    .start()

# COMMAND ----------

display(spark.sql("SELECT * FROM {}.{}".format(agg_db, agg_table)))

# COMMAND ----------

df = spark.sql("SELECT * FROM {}.{}".format(target_db, target_table))
display(df.select('category', 'val').groupBy('category').agg({'val': 'sum'}).withColumnRenamed('SUM(val)', 'sum'))

# COMMAND ----------

# Follow docs example at
# https://docs.databricks.com/delta/delta-update.html#writing-streaming-aggregates-in-update-mode-using-merge-and-foreachbatch-notebook

prev_time = curr_time
curr_time += 1

#     row_type('key1', 'cat1', 1, curr_time, 'part1', ''),
#     row_type('key2', 'cat1', 2, curr_time, 'part1', ''),
#     row_type('key3', 'cat2', 3, curr_time, 'part2', ''),
#     row_type('key4', 'cat2', 4, curr_time, 'part2', ''),
#     row_type('key5', 'cat3', 5, curr_time, 'part3', ''),
#     row_type('key6', 'cat3', 6, curr_time, 'part3', ''),


# row_type = pyspark.sql.Row('key', 'val', 'timestamp', 'part', 'deleted')
update_df = spark.createDataFrame([
    # Delete
    row_type('key2', 'cat1', 2, curr_time, 'part1', 'Y'),
    # Update
    row_type('key3', 'cat2', 5, curr_time, 'part2', ''),
    # Insert
    row_type('key7', 'cat3', 7, curr_time, 'part3', ''),
    row_type('key8', 'cat4', 8, curr_time, 'part3', ''),
])
display(update_df)

# COMMAND ----------

# merge into target table
from delta.tables import *

deltaTable = DeltaTable.forPath(spark, target_loc + '_full')

deltaTable.alias("target").merge(
    update_df.alias("updates"),
    "target.key = updates.key") \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll() \
  .execute()

# COMMAND ----------

df = spark.sql("SELECT * FROM {}.{}".format(target_db, target_table + '_full'))
display(df)

# COMMAND ----------

# Start the query to upsert once into aggregates tables in update mode
agg_df.writeStream \
    .trigger(once=True) \
    .format("delta") \
    .foreachBatch(upsertToDelta) \
    .outputMode("update") \
    .start()

# COMMAND ----------

display(spark.sql("SELECT * FROM {}.{}".format(agg_db, agg_table)))

# COMMAND ----------


