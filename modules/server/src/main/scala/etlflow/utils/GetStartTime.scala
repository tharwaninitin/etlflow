package etlflow.utils


import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId}

private [etlflow] object GetStartTime {
  def apply(startTime:Option[java.time.LocalDate]): Long = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    if (startTime.isDefined)
      startTime.get.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli()
    else
      sdf.parse(LocalDate.now().toString).getTime
  }
}
