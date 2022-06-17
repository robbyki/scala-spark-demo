// Databricks notebook source
import scala.util.Random._
import org.apache.spark.sql._
import org.apache.spark.Partition
import org.apache.spark.sql.functions._

val spark = SparkSession.builder
  .appName("SparkSQL")
  .master("local[4]") //only for local mode
  .getOrCreate()

import spark.implicits._

val mydf = Seq(
  Array("123", "abc", "2017", "ABC"),
  Array("456", "def", "2001", "ABC"),
  Array("789", "ghi", "2017", "DEF")
).toDF("col")

mydf.show(false)

mydf
  .withColumn("col1", element_at('col, 1))
  .withColumn("col2", element_at('col, 2))
  .withColumn("col3", element_at('col, 3))
  .withColumn("col4", element_at('col, 4))
  .drop('col)
  .show()

mydf.printSchema()

