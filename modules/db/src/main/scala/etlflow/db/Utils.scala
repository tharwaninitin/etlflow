package etlflow.db

import org.postgresql.util.PGobject
import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId}

object Utils {
  def getStartTime(startTime:Option[java.time.LocalDate]): Long = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    if (startTime.isDefined)
      startTime.get.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli()
    else
      sdf.parse(LocalDate.now().toString).getTime
  }
  def convertToPGJson(rs: String): PGobject = {
    val jsonObject = new PGobject()
    jsonObject.setType("json")
    jsonObject.setValue(rs)
    jsonObject
  }
}
