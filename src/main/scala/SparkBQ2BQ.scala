package com.zero

import com.zero.util.ConnectionsCommons._
import org.apache.spark.sql._
import com.google.cloud.spark.bigquery._
import com.databricks.spark.avro._
import com.google.cloud.spark.bigquery.repackaged.com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.JobInfo.WriteDisposition
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.{BigQuery, BigQueryOptions, FormatOptions, Job, JobInfo, LoadJobConfiguration, Table, TableId}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SparkBQ2BQ {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
                  .appName("bq2bq")
                  .master("local[*]")
                  .config("spark.driver.host", "localhost")
                  .enableHiveSupport()
                  .getOrCreate()

    spark.sparkContext.setLogLevel( "WARN" )
    spark.conf.set("viewsEnabled","true")

    import com.google.cloud.spark.bigquery.BigQueryDataFrameReader
    println("starting to read..")

    val df = spark.read.bigquery("datalake.dataset.table")

    println("starting to write..")

    df
      .write
      .format("parquet")
      .save("data/")

    println("done..")

  }

}
