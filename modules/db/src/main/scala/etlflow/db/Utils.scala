package etlflow.db

import scalikejdbc.{NoExtractor, SQL}
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
  def getSqlQueryAsString[T](sqlQuery: SQL[T, NoExtractor]): String = {
    val statement = sqlQuery.statement
    val params = sqlQuery.parameters.map { value =>
      if (value == null) "null" else value.toString
    }
    params.foldLeft(statement) { (text, param) =>
      text.replaceFirst("\\?", param)
    }
  }
}
