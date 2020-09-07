package etlflow.spark

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, from_unixtime, udf, unix_timestamp}

trait SparkUDF {

  def get_24hr_formatted_from_12hr (startTime:String) : Option[String] = {
    val StartTime = Option(startTime).getOrElse(return None).toString
    val meridiem = StartTime.split(" ")(1)
    val time = StartTime.split(" ")(0).replace(":","")
    val hour = StartTime.split(" ")(0).split(":")(0)
    val FormattedTime= meridiem match {
      case x if x == "PM" && (hour.toInt != 12) => (time.toInt+120000)
      case x if x == "PM" && (hour.toInt == 12) => time
      case x if x == "AM" && (hour.toInt == 12) => "00"+(time.toInt-120000)
      case x if x == "AM" && (hour.toInt != 12) => time
    }
    val time_new=FormattedTime.toString
    val time_split = if (time_new.length == 6) time_new.splitAt(2) else time_new.splitAt(1)
    val hours = if (time_split._1.length ==2 ) time_split._1 else "0"+time_split._1
    val minutes = time_split._2.splitAt(2)._1
    val seconds = time_split._2.splitAt(2)._2
    Some(hours+":"+minutes+":"+seconds)
  }

  def get_24hr_formatted (input_time:String) : Option[String] = {
    val time = Option(input_time).getOrElse(return None)
    val time_split = if (time.length == 6) time.splitAt(2) else time.splitAt(1)
    val hours = if (time_split._1.length ==2 ) time_split._1 else "0"+time_split._1
    val minutes = time_split._2.splitAt(2)._1
    val seconds = time_split._2.splitAt(2)._2
    Some(hours+":"+minutes+":"+seconds)
  }

  val get_formatted_date: (String,String,String) => Column
  = (ColumnName:String,ExistingFormat:String,NewFormat:String) => {
    from_unixtime(unix_timestamp(col(ColumnName), ExistingFormat), NewFormat)
  }
  val get_24hr_formatted_from_12hr_udf  = udf[Option[String],String](get_24hr_formatted_from_12hr)
  val get_24hr_formatted_udf  = udf[Option[String],String](get_24hr_formatted)
}
