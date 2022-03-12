package etlflow.spark

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, from_unixtime, udf, unix_timestamp}

trait SparkUDF {

  def get24hrFrom12hr(startTime: String): Option[String] =
    if (startTime == null) None
    else {
      val meridiem = startTime.split(" ")(1)
      val time     = startTime.split(" ")(0).replace(":", "")
      val hour     = startTime.split(" ")(0).split(":")(0)
      val formattedTime: String = meridiem match {
        case x if x == "PM" && (hour.toInt != 12) => (time.toInt + 120000).toString
        case x if x == "PM" && (hour.toInt == 12) => time
        case x if x == "AM" && (hour.toInt == 12) => "00" + (time.toInt - 120000).toString
        case x if x == "AM" && (hour.toInt != 12) => time
      }
      val timeSplit = if (formattedTime.length == 6) formattedTime.splitAt(2) else formattedTime.splitAt(1)
      val hours     = if (timeSplit._1.length == 2) timeSplit._1 else "0" + timeSplit._1
      val minutes   = timeSplit._2.splitAt(2)._1
      val seconds   = timeSplit._2.splitAt(2)._2
      Some(hours + ":" + minutes + ":" + seconds)
    }

  def get24hr(inputTime: String): Option[String] =
    if (inputTime == null) None
    else {
      val timeSplit = if (inputTime.length == 6) inputTime.splitAt(2) else inputTime.splitAt(1)
      val hours     = if (timeSplit._1.length == 2) timeSplit._1 else "0" + timeSplit._1
      val minutes   = timeSplit._2.splitAt(2)._1
      val seconds   = timeSplit._2.splitAt(2)._2
      Some(hours + ":" + minutes + ":" + seconds)
    }

  val getFormattedDate: (String, String, String) => Column = (ColumnName: String, ExistingFormat: String, NewFormat: String) =>
    from_unixtime(unix_timestamp(col(ColumnName), ExistingFormat), NewFormat)
  val get24hrFrom12hrUdf = udf[Option[String], String](get24hrFrom12hr)
  val get24hrUdf         = udf[Option[String], String](get24hr)
}
