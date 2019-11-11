// Databricks notebook source
display(dbutils.fs.ls("dbfs:/databricks-datasets/definitive-guide/data/activity-data/*.json"))

// COMMAND ----------

dbutils.fs.help

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/sal_datasets/flight_dataset/flightDataset.parquet/"))

// COMMAND ----------

dbutils.fs.mv("dbfs:/SAL_FLIGHT_DATASET/","dbfs:/sal_datasets/flight_dataset",true)

// COMMAND ----------

spark.read.format("")

// COMMAND ----------

import org.apache.spark.sql.functions._
val data=spark.read.format("json").load("dbfs:/databricks-datasets/definitive-guide/data/activity-data/*.json")
data.cache



// COMMAND ----------

val new_data=data.withColumn("new_creation_time",(($"Creation_Time".cast("Double"))/(lit(1000000000))).cast("timestamp"))
new_data.repartition(1).write.format("parquet").save("dbfs:/sal_datasets/activity_dataset/activity.parquet")


// COMMAND ----------

val activitySchema=spark.read.format("parquet").load("dbfs:/sal_datasets/activity_dataset/activity.parquet").schema

// COMMAND ----------

val sampleData=spark.read.format("parquet").load("dbfs:/sal_datasets/activity_dataset/activity.parquet").limit(10)
display(sampleData.select($"new_creation_time".cast("date").alias("dates")).select($"dates").distinct)

// COMMAND ----------

val streamQuery=spark.readStream.format("parquet").schema(activitySchema).load("dbfs:/sal_datasets/activity_dataset/activity.parquet")

// COMMAND ----------

//tumbling window example
val streamGroupBy=streamQuery.filter($"gt"=!="null").groupBy(window($"new_creation_time","24 hours"),$"gt").count

// COMMAND ----------

//sliding window example
val streamGroupBySlidingWindow=streamQuery.filter($"gt"=!="null").groupBy(window($"new_creation_time","2 minutes","1 minutes"),$"gt").count

// COMMAND ----------

val finalSlidingWindowQuery=streamGroupBySlidingWindow.writeStream.format("memory").queryName("activityTableSldingWindow").outputMode("complete").start()

// COMMAND ----------

spark.sql("select * from activityTableSldingWindow").show

// COMMAND ----------

//with watermark
//sliding window example
val streamGroupBySlidingWindowWithWatermark=streamQuery.filter($"gt"=!="null").withWatermark("new_creation_time","2 minutes").groupBy(window($"new_creation_time","2 minutes","1 minutes"),$"gt").count

// COMMAND ----------



// COMMAND ----------

val queryWithWatermark=streamGroupBySlidingWindowWithWatermark.writeStream.format("memory").queryName("val queryWithWatermark=streamGroupBySlidingWindowWithWatermark.writeStream.format("memory").queryName("activityTableWithWatermark").outputMode("complete").start()").outputMode("complete").start()

// COMMAND ----------

spark.sql("select * from activityTableWithWatermark").show

// COMMAND ----------

val finalQuery=streamGroupBy.writeStream.format("memory").queryName("activityTable").outputMode("complete").start()

// COMMAND ----------

spark.sql("select * from activityTable").show