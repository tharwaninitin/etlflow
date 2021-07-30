package etlflow.utils

import cron4s.Cron
import cron4s.lib.javatime._
import etlflow.api.Schema.Job
import etlflow.db.JobDBAll
import etlflow.utils.DateTimeApi.{getCurrentTimestampUsingLocalDateTime, getLocalDateTimeFromTimestamp, getTimeDifferenceAsString, getTimestampFromLocalDateTime}
import org.ocpsoft.prettytime.PrettyTime

import java.time.LocalDateTime

object GetCronJob {
  def apply(schedule: String, jdb:JobDBAll, lastRunTime:String, props:Map[String,String]):Job = {
    val pt = new PrettyTime()
    if (Cron(schedule).toOption.isDefined) {
      val cron = Cron(schedule).toOption
      val startTimeMillis: Long = getCurrentTimestampUsingLocalDateTime
      val endTimeMillis: Option[Long] = cron.get.next(LocalDateTime.now()).map(dt => getTimestampFromLocalDateTime(dt))
      val remTime1 = endTimeMillis.map(ts => getTimeDifferenceAsString(startTimeMillis, ts)).getOrElse("")
      val remTime2 = endTimeMillis.map(ts => pt.format(getLocalDateTimeFromTimestamp(ts))).getOrElse("")
      val nextScheduleTime = cron.get.next(LocalDateTime.now()).getOrElse("").toString
      Job(jdb.job_name, props, cron, nextScheduleTime, s"$remTime2 ($remTime1)", jdb.failed, jdb.success, jdb.is_active, jdb.last_run_time.getOrElse(0), s"$lastRunTime")
    } else {
      Job(jdb.job_name, props, None, "", "", jdb.failed, jdb.success, jdb.is_active, jdb.last_run_time.getOrElse(0), s"$lastRunTime")
    }
  }
}
