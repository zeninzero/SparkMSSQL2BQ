
package com.paloaltonetworks.bi.util

import java.util.Calendar

object MiscUtil {

  import java.time.format.DateTimeFormatter
  val datetime_format = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'.000+0000'")
  val datetime_format_simple = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  val datetime_format_short = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm")

  def inform(s: String, withTimeMark: Boolean = true): Unit = {
    val timePart = if (withTimeMark) Calendar.getInstance().getTime.toString else ""

    println(s"$timePart $s")
  }

  def convertMilSecondsToHMmSs(milSeconds: Long): String = {
    val seconds: Long = milSeconds / 1000
    val s = seconds % 60
    val m = (seconds / 60) % 60
    val h = (seconds / (60 * 60)) % 24
    f"$h%d:$m%02d:$s%02d"
  }


  // if need to merge 2 generic columns in dataframe
  import org.apache.spark.sql.functions.udf
  // type tag is required for a generic udf
  import scala.reflect.runtime.universe.{TypeTag, typeTag}
  def toTuple2[S: TypeTag, T: TypeTag] =
      udf[(S, T), S, T]((x: S, y: T) => (x, y))

}

