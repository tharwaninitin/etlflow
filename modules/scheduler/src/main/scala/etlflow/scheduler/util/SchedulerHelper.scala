package etlflow.scheduler.util

import java.text.SimpleDateFormat
import java.time._
object SchedulerHelper {

  def getValidDates(start_date:Option[String],end_date:Option[String]): (Long,Long) ={
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    var start_date_arg = start_date.get
    if(start_date.get == "") {
      start_date_arg = LocalDate.now().plusDays(1).toString
    }
    val endTime   = sdf.parse(end_date.getOrElse("")).getTime
    val startTime = sdf.parse(start_date_arg).getTime
    (startTime, endTime)
  }
}
