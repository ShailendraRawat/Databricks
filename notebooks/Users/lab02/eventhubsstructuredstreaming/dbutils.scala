// Databricks notebook source
dbutils.fs.mv("/images/cosmic_timetraveler_LhDWW8PhPoE_unsplash-abc36.jpg","/FileStore/tables/",true)

// COMMAND ----------

display(dbutils.fs.ls("/images/"))

// COMMAND ----------

display(dbutils.fs.ls("/images/"))

// COMMAND ----------

// MAGIC %md [test_image]("dbfs:/FileStore/tables/cosmic_timetraveler_LhDWW8PhPoE_unsplash-abc36.jpg")

// COMMAND ----------

val data=spark.read.format("parquet").option("compresion","snappy").load("dbfs:/databricks-datasets/credit-card-fraud/data/part-00007-tid-898991165078798880-9c1caa7b-283d-47c4-9be1-aa61587b3675-0-c000.snappy.parquet")

// COMMAND ----------

display(dbutils.fs.ls
        ()
       )

// COMMAND ----------

dbutils.fs.ls

// COMMAND ----------

// MAGIC %md [imaage]("starry_sky_tree_sand_124955_1920x1080.jpg")

// COMMAND ----------

display(data.limit(10))

// COMMAND ----------

val times=data.rdd.map(x=>x(0).toString).take(1023)

// COMMAND ----------

dbutils.widgets.dropdown("time_dropdown","34032",for (x <- times) yield x )

// COMMAND ----------

dbutils.widgets.get("time_dropdown")

// COMMAND ----------

val data=spark.read.format("csv").option("header","true").option("seperator","/t").load("/databricks-datasets/power-plant/data/*.tsv")

// COMMAND ----------

data.show(truncate=false)

// COMMAND ----------

widgets.help("text")