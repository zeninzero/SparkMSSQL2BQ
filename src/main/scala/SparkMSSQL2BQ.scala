package com.zero

import com.zero.config.ApplicationConfiguration
import com.zero.config.ApplicationConfiguration._
import com.zero.util.ConnectionsCommons._
import com.zero.util.{JdbcClient, MiscUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkMSSQL2BQ {

  implicit val programName: ProgramName = this.getClass.getSimpleName.init

  def main(args: Array[String]) {

    val startTime = System.currentTimeMillis()

    Logger.getRootLogger.setLevel(Level.WARN)
    Logger.getLogger(programName).setLevel(Level.INFO)

    // Read Cluster config and Application config
    ApplicationConfiguration(args(0))

    val jdbcData = JdbcClient.getTableDetails("org.postgresql.Driver",
      "jdbc:postgresql://",
      "","","a.b","")

    val rankColumn = jdbcData._1
    val upperBound = jdbcData._2

    println("rankColumn::"+rankColumn)
    println("upperBound::"+upperBound)

    MiscUtil.inform("***************************************************")
    MiscUtil.inform(s"Process Started: $programName")
    MiscUtil.inform("***************************************************")

    implicit val spark: SparkSession = createSparkSession(programName)

    spark.sparkContext.setLogLevel("WARN")

    try {

      // get values from config into Spark and read source
      val jdbcDF = spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://")
        .option("user", "")
        .option("password", "")
        .option("driver","org.postgresql.Driver")
        //.option("dbtable","select * from (SELECT inn.*,row_number() OVER (partition by SegmentNum order by StartDateTimeUTC) AS num_rows FROM (SELECT * FROM dbo.IO_Activity) inn ) as tmp" )
        .option("dbtable","gcsd.case_cost_audit")
        .option("numPartitions", hanaNumPartitions)
        //.option("partitionColumn","num_rows")
        .option("lowerBound",0)
        .option("upperBound",upperBound)
        .option("fetchsize", hanaFetchSize)
        .load()

      //jdbcDF.drop("num_rows").createOrReplaceTempView("source_data")
      jdbcDF.createOrReplaceTempView("source_data")

      val finalResult = spark.sql(hanaQuery)

      MiscUtil.inform("++++ Reading source completed.")

      var df = finalResult

      finalResult.columns.foreach( c =>
        df = df.withColumnRenamed( c, c.toLowerCase.replaceAll( "/", "_" ).replaceAll( " ", "_" ) )
      )

      MiscUtil.inform("++++ Preparing data completed.")

      df.repartition(numPartitions = sparkDfPartitions)

      df.show(20,false)
      df.write.format("com.databricks.spark.avro")
        .mode(SaveMode.Overwrite)
        //.save(googleBucket+"i3")
        .parquet(googleBucket+"postgres_test")

      //.save("/Users/vmadhira/Desktop/test/")

      MiscUtil.inform("++++ Writing data to destination completed.")

      safeExit(exitCode = 0, finalResult.count())

    } catch {
      case e: Exception =>
        e.printStackTrace()
        safeExit(1)
    }


    def safeExit(exitCode: Int, count: Long = 0)(implicit spark: SparkSession): Unit = {

      if (exitCode == 0) {
        val duration = MiscUtil.convertMilSecondsToHMmSs(System.currentTimeMillis() - startTime)
        val finalMessage = s"Process Finished $programName ($count rows) in $duration"

        MiscUtil.inform("***************************************************")
        MiscUtil.inform(finalMessage)
        MiscUtil.inform("***************************************************")
      }
      spark.close()
      sys.exit(exitCode)
    }

  }
}



