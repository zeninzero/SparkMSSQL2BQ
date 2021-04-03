package com.zero.util

import com.zero.config.ApplicationConfiguration.ProgramName
import org.apache.spark.sql.SparkSession

object ConnectionsCommons {
  private var _spark: SparkSession = _

  // singleton
  def createSparkSession(programName: ProgramName) = {
    if (_spark == null) {
      _spark = SparkSession.builder()
        .master("local[*]")
        .appName(programName)
        .config("spark.driver.host", "localhost")
        .enableHiveSupport()
        .getOrCreate()
    }
    _spark
  }


}
