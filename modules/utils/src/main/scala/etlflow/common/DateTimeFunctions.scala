package etlflow.common

import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.TimeZone
import scala.util.{Failure, Success, Try}

private[etlflow] object DateTimeFunctions {

  def getCurrentTimestamp: Long = System.currentTimeMillis()
  def getCurrentTimestampUsingLocalDateTime: Long = LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant.toEpochMilli
  // https://stackoverflow.com/questions/23944370/how-to-get-milliseconds-from-localdatetime-in-java-8
  def getTimestampFromLocalDateTime(dt: LocalDateTime): Long = dt.atZone(ZoneId.systemDefault()).toInstant.toEpochMilli
  def getLocalDateTimeFromTimestamp(ts: Long): LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts),ZoneId.systemDefault())
  // https://stackoverflow.com/questions/24806183/get-date-in-current-timezone-in-java
  def getCurrentTimestampAsString(pattern: String = "yyyy-MM-dd HH:mm:ss"): String =
    DateTimeFormatter.ofPattern(pattern).format(LocalDateTime.now) + " " + TimeZone.getDefault.getDisplayName(false, TimeZone.SHORT)
  // https://stackoverflow.com/questions/4142313/convert-timestamp-in-milliseconds-to-string-formatted-time-in-java
  def getTimestampAsString(timestamp: Long, pattern: String = "yyyy-MM-dd HH:mm:ss"): String =
    DateTimeFormatter.ofPattern(pattern).format(LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp),ZoneId.systemDefault())) + " " + TimeZone.getDefault.getDisplayName(false, TimeZone.SHORT)

  def roundAt(p: Int)(n: Double): Double = { val s = math pow (10, p); (math round n * s) / s }

  def getTimeDifferenceAsString(start_ts: Long, end_ts: Long): String = {
    Try((end_ts - start_ts) / 1000.0).map{value =>
      if (value > 86400) {
        val diff = value % 86400
        roundAt(2)(value/86400).toInt + " days " + roundAt(2)(diff/3600.0) + " hrs"
      }
      else if (value > 3600 && value < 86400) roundAt(2)(value/3600.0) + " hrs"
      else if (value > 60 && value < 3600) roundAt(2)(value/60.0) + " mins"
      else roundAt(2)(value) + " secs"
    } match {
      case Success(value) => value
      case Failure(e) => (end_ts - start_ts).toString
    }
  }

}
