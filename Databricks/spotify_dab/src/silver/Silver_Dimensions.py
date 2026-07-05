# Databricks notebook source
# MAGIC %md
# MAGIC ## **DimUser**

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

import os
import sys

project_path = os.path.abspath(
    os.path.join(os.getcwd(), '..', '..')
)

print(project_path)

sys.path.append(project_path)

from utils.transformations import reusable

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Auto Loader**

# COMMAND ----------

df_user = spark.readStream.format("cloudFiles")\
                          .option("cloudFiles.format","parquet")\
                          .option("cloudFiles.schemaLocation",
"abfss://silver@datalakeazure.dfs.core.windows.net/DimUser/checkpoint")\
    .load("abfss://bronze@datalakeazure.dfs.core.windows.net/DimUser")

# COMMAND ----------

display(
  spark.read.format("parquet")
  .load("abfss://bronze@datalakeazure.dfs.core.windows.net/DimUser")
)

# COMMAND ----------

display(
    spark.read.format("parquet")
    .load("abfss://bronze@datalakeazure.dfs.core.windows.net/DimUser")
    .withColumn("user_name", upper(col("user_name")))
)

# COMMAND ----------

df_user.printSchema()

# COMMAND ----------

df_user_obj=reusable()
df_user=df_user_obj.dropColumns(df_user,['_rescued_data'])
df_user=df_user.dropDuplicates(['user_id'])

# COMMAND ----------

df_user.printSchema()

# COMMAND ----------

print(df_user.isStreaming)

# COMMAND ----------

df_user=df_user.writeStream.format("delta")\
                           .outputMode("append")\
                           .option("checkpointLocation","abfss://silver@datalakeazure.dfs.core.windows.net/DimUser/checkpoint")\
                           .trigger(once=True)\
                           .option("path","abfss://silver@datalakeazure.dfs.core.windows.net/DimUser/data")\
                           .toTable("spotify_cata.silver.DimUser")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DimArtist**

# COMMAND ----------

df_artist = spark.readStream.format("cloudFiles")\
                          .option("cloudFiles.format","parquet")\
                          .option("cloudFiles.schemaLocation",
"abfss://silver@datalakeazure.dfs.core.windows.net/DimArtist/checkpoint")\
    .load("abfss://bronze@datalakeazure.dfs.core.windows.net/DimArtist")

# COMMAND ----------

df_artist.printSchema()

# COMMAND ----------

df_artist_obj=reusable()
df_artist=df_artist_obj.dropColumns(df_artist,['_rescued_data'])
df_artist=df_artist.dropDuplicates(['artist_id'])

# COMMAND ----------

df_artist.printSchema()

# COMMAND ----------

df_artist=df_artist.writeStream.format("delta")\
                           .outputMode("append")\
                           .option("checkpointLocation","abfss://silver@datalakeazure.dfs.core.windows.net/DimArtist/checkpoint")\
                           .trigger(once=True)\
                           .option("path","abfss://silver@datalakeazure.dfs.core.windows.net/DimArtist/data")\
                           .toTable("spotify_cata.silver.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DimTrack**

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles")\
                          .option("cloudFiles.format","parquet")\
                          .option("cloudFiles.schemaLocation",
"abfss://silver@datalakeazure.dfs.core.windows.net/DimTrack/checkpoint")\
    .load("abfss://bronze@datalakeazure.dfs.core.windows.net/DimTrack")

# COMMAND ----------

display(
  spark.read.format("parquet")
  .load("abfss://bronze@datalakeazure.dfs.core.windows.net/DimTrack")
)

# COMMAND ----------

df_track=df_track.withColumn("duration_Flag",when(col('duration_sec')<150,'low')\
                             .when(col('duration_sec')<150,'medium')\
                                   .otherwise('high'))


# COMMAND ----------

df_track.printSchema()

# COMMAND ----------

df_track=df_track.withColumn("track_name",regexp_replace(col("track_name"),"-"," "))
df_track=df_track.dropDuplicates(['track_id'])
df_track=reusable().dropColumns(df_track,['_rescued_data'])
df_track.printSchema()


# COMMAND ----------

df_track=df_track.writeStream.format("delta")\
                           .outputMode("append")\
                           .option("checkpointLocation","abfss://silver@datalakeazure.dfs.core.windows.net/DimTrack/checkpoint")\
                           .trigger(once=True)\
                           .option("path","abfss://silver@datalakeazure.dfs.core.windows.net/DimTrack/data")\
                           .toTable("spotify_cata.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DimDate**

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles")\
                          .option("cloudFiles.format","parquet")\
                          .option("cloudFiles.schemaLocation",
"abfss://silver@datalakeazure.dfs.core.windows.net/DimDate/checkpoint")\
    .load("abfss://bronze@datalakeazure.dfs.core.windows.net/DimDate")

# COMMAND ----------

display(
  spark.read.format("parquet")
  .load("abfss://bronze@datalakeazure.dfs.core.windows.net/DimDate")
)

# COMMAND ----------

df_date=reusable().dropColumns(df_date,['_rescued_data'])

# COMMAND ----------

df_date=df_date.writeStream.format("delta")\
                           .outputMode("append")\
                           .option("checkpointLocation","abfss://silver@datalakeazure.dfs.core.windows.net/DimDate/checkpoint")\
                           .trigger(once=True)\
                           .option("path","abfss://silver@datalakeazure.dfs.core.windows.net/DimDate/data")\
                           .toTable("spotify_cata.silver.Dimdate")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **FactStream**

# COMMAND ----------

df_stream = spark.readStream.format("cloudFiles")\
                          .option("cloudFiles.format","parquet")\
                          .option("cloudFiles.schemaLocation",
"abfss://silver@datalakeazure.dfs.core.windows.net/FactStream/checkpoint")\
    .load("abfss://bronze@datalakeazure.dfs.core.windows.net/FactStream")

# COMMAND ----------

df_stream.printSchema()

# COMMAND ----------

df_stream=reusable().dropColumns(df_stream,['_rescued_data'])
df_stream=df_stream.writeStream.format("delta")\
                           .outputMode("append")\
                           .option("checkpointLocation","abfss://silver@datalakeazure.dfs.core.windows.net/FactStream/checkpoint")\
                           .trigger(once=True)\
                           .option("path","abfss://silver@datalakeazure.dfs.core.windows.net/FactStream/data")\
                           .toTable("spotify_cata.silver.FactStream")
