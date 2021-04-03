package com.zero

import com.google.cloud.spark.bigquery.repackaged.org.apache.avro.data.TimeConversions
import com.zero.config.ApplicationConfiguration
import com.zero.config.ApplicationConfiguration._
import com.zero.util.ConnectionsCommons._
import com.zero.util.{JdbcClient, MiscUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.joda.time.format.DateTimeFormat

object SparkHANA2BQ {

  implicit val programName: ProgramName = this.getClass.getSimpleName.init

  def main(args: Array[String]) {

    val startTime = System.currentTimeMillis()

    Logger.getRootLogger.setLevel(Level.WARN)
    Logger.getLogger(programName).setLevel(Level.INFO)

    val usage = """
    Usage: spark2-submit \
    |--driver-memory 10G \
    |--executor-memory=10G \
    |--master=yarn --num-executors=10 \
    |--conf spark.network.timeout=1600 \
    |--conf spark.rpc.askTimeout=1600 \
    |DataSlurper-uber.jar \
    |“vbfa.conf” \
    |--hanaDbTable "SFDC.USER" \
    """

    if (args.length == 0) {
      println(usage)
    }

    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]
    def isSwitch(s : String) = (s(0) == '-')

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      list match {
        case Nil => map
        case "--hanaDbTable" :: value :: tail =>
              nextOption(map ++ Map('hanaDbTable -> value.toString), tail)
        case "--min-size" :: value :: tail =>
              nextOption(map ++ Map('minsize -> value.toInt), tail)
        case string :: opt2 :: tail if isSwitch(opt2) =>
              nextOption(map ++ Map('infile -> string), list.tail)
        case string :: Nil =>  nextOption(map ++ Map('infile -> string), list.tail)
      }
    }

    val options = nextOption(Map(),arglist)

    // Read Cluster config and Application config
    ApplicationConfiguration(args(0))

    val jdbcData = JdbcClient.getTableDetails(hanaDriver,hanaURL,hanaUserName,hanaPassword,options('hanaDbTable).toString,whereClause)

    val rankColumn = jdbcData._1
    val upperBound = jdbcData._2

    MiscUtil.inform("***************************************************")
    MiscUtil.inform(s"Process Started: $programName")
    MiscUtil.inform("***************************************************")

    implicit val spark: SparkSession = createSparkSession(programName)

    spark.sparkContext.setLogLevel("WARN")

    try {

      // get values from config into Spark and read source
      val jdbcDF = spark.read
        .format("jdbc")
        .option("url", hanaURL)
        .option("user", hanaUserName)
        .option("password", hanaPassword)
        .option("driver",hanaDriver)
        //.option("dbtable","(SELECT ref.*,row_number() OVER ( ORDER BY "+rankColumn+" ) AS num_rows FROM (SELECT * FROM "+options.get('hanaDbTable).get.toString+whereClause+") ref ) tmp" )
        .option("dbtable",options.get('hanaDbTable).get.toString)
        .option("numPartitions", hanaNumPartitions)
        //.option("partitionColumn","num_rows")
        .option("lowerBound",0)
        .option("upperBound",upperBound)
        .option("fetchsize", hanaFetchSize)
        .load()

      jdbcDF.drop("num_rows").createOrReplaceTempView("source_data")
      //jdbcDF.createOrReplaceTempView("source_data")

      val finalResult = spark.sql(hanaQuery)

      MiscUtil.inform("++++ Reading source completed.")

      var df = finalResult

      finalResult.columns.foreach( c =>
        df = df.withColumnRenamed( c, c.toLowerCase.replaceAll( "/", "_" ).replaceAll( " ", "_" ) )
      )

      MiscUtil.inform("++++ Preparing data completed.")

      df.repartition(numPartitions = sparkDfPartitions)

      df.write
        .mode(SaveMode.Overwrite)
        .parquet(googleBucket+options.get('hanaDbTable).get.toString)
        //.parquet("data/")

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



