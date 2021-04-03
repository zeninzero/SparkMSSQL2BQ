package com.zero.config

import java.io.File
import com.typesafe.config.{Config, ConfigFactory}

object ApplicationConfiguration {

  type ProgramName = String

  lazy val sourceType = applicationConfig.getString("source.type")

  lazy val hanaURL = applicationConfig.getString("hana.url")
  lazy val hanaDriver = applicationConfig.getString("hana.driver")
  //lazy val hanaDbTable = applicationConfig.getString("hana.dbtable")
  //lazy val rankColumn = applicationConfig.getString("hana.rankColumn")
  lazy val hanaUserName = applicationConfig.getString("hana.username")
  lazy val hanaPassword = applicationConfig.getString("hana.password")
  lazy val hanaNumPartitions = applicationConfig.getString("hana.numPartitions")
  lazy val numPartitions = applicationConfig.getString("hana.numPartitions").toInt
  lazy val hanaFetchSize = applicationConfig.getString("hana.fetchsize")
  lazy val hanaQuery = applicationConfig.getString("hana.query")
  lazy val googleBucket = applicationConfig.getString("google.bucket")
  lazy val googleAuth = applicationConfig.getString("google.auth")
  lazy val googleProject = applicationConfig.getString("google.project")
  lazy val sparkDfPartitions = applicationConfig.getInt("spark.dataframe.partitions")
  lazy val kuduConnection = applicationConfig.getString("kudu.connection")
  lazy val kuduTable = applicationConfig.getString("kudu.table")
  lazy val isPipeline = applicationConfig.getBoolean("spark.isPipeline")
  lazy val whereClause = applicationConfig.getString("hana.whereClause")


  private var applicationConfig: Config = _

  def apply(s: String): Unit = {
    val file = new File(s)
    applicationConfig = ConfigFactory.parseFile(file)
  }
}

