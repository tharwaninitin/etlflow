package etljobs.functions

import java.util.Calendar

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

trait SparkUDF {

  def get_12hr_formatted(startTime:String) : Option[String] = {
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

  val get_formatted_date : (String,String,String) => Column
  = (ColumnName:String,ExistingFormat:String,NewFormat:String) => {
    from_unixtime(unix_timestamp(col(ColumnName), ExistingFormat), NewFormat)
  }

  val get_barc_year = (current_day : String ) => {
    val inputFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val calendar_instance = Calendar.getInstance()
    calendar_instance.setTime(inputFormat.parse(current_day))

    var week = calendar_instance.get(Calendar.WEEK_OF_YEAR)
    val day = calendar_instance.get(Calendar.DAY_OF_WEEK)
    var year = calendar_instance.get(Calendar.YEAR)
    val month = calendar_instance.get(Calendar.MONTH)

    if (day == 7) week = week + 1
    if (week == 53 || (week == 1 && month == 11)) {
      week = 1
      year = year + 1
    }

    year.toString
  }

  val get_barc_week = (current_day : String ) => {
    val inputFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val calendar_instance = Calendar.getInstance()
    calendar_instance.setTime(inputFormat.parse(current_day))

    var week = calendar_instance.get(Calendar.WEEK_OF_YEAR)
    val day = calendar_instance.get(Calendar.DAY_OF_WEEK)
    var year = calendar_instance.get(Calendar.YEAR)
    val month = calendar_instance.get(Calendar.MONTH)

    if (day == 7) week = week + 1
    if (week == 53 || (week == 1 && month == 11)) {
      week = 1
      year = year + 1
    }

    week.toString
  }

  val time_band_split = (air_time : String) => {
    if(air_time=="NULL" || air_time=="0" || air_time=="null" || air_time==""){
      "0000-0030"
    }
    else{
      val air_time_split = if(air_time.length==6) air_time.splitAt(2) else air_time.splitAt(1)
      val hours = if(air_time_split._1.length>1) air_time_split._1 else "0"+air_time_split._1
      var minutes = air_time_split._2.splitAt(2)._1
      if (minutes.splitAt(1)._1 == 0.toString)
        minutes = minutes.splitAt(1)._2
      else
        minutes
      val start_time_hours = hours
      if (minutes.toInt >= 0 && minutes.toInt < 30) {
        val start_time_start_minutes = "00"
        val start_time_end_minutes = "30"
        start_time_hours + "" + start_time_start_minutes + "-" + start_time_hours + "" + start_time_end_minutes
      }
      else {
        val start_time_start_minutes = "30"
        val start_time_end_minutes = "00"
        val end_hours=if((start_time_hours.toInt + 1).toString.length==2) (start_time_hours.toInt + 1) else "0"+(start_time_hours.toInt + 1)
        start_time_hours + "" + start_time_start_minutes + "-" + end_hours  + "" + start_time_end_minutes
      }}
  }
  val time_band_split_regional = (timeband :String)=> {
    val timeband_array: Array[String] = timeband.split("-")
    timeband_array(0)
  }
  val get_barc_year_udf = udf(get_barc_year)
  val get_barc_week_udf = udf(get_barc_week)
  val time_band_split_udf = udf(time_band_split)
  val time_band_split_regional_udf = udf(time_band_split_regional)

  def get_grp_tvt(numerator:Column,denominator:Column) : Column = (numerator*denominator)/10
  def retrofit_column(in_parameter : Column) : Column = upper(regexp_replace(in_parameter," ",""))
  def get_round_value(column:Column) : Column = round(column,6)

  val get_12hr_formatted_udf  = udf[Option[String],String](get_12hr_formatted)
  val get_24hr_formatted_udf  = udf[Option[String],String](get_24hr_formatted)


}